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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the <b>public shape</b> of M15/P14's async admin flavour — <see cref="IAsyncAdmin"/>, its two clients and
/// the 44 <c>Async*Result</c> types — against the flavour it twins (<see cref="IAdmin"/> and today's Results).
/// </summary>
/// <remarks>
/// The flavours must offer the same RPCs and the same Result members, differing only in the future type at the
/// public boundary. These tests compare the two by reflection with every future type folded into one
/// flavour-neutral token, so a member added to, dropped from or re-typed on one flavour only turns them red.
/// </remarks>
public sealed class PublicAdminP14SurfaceTests
{
    private const string AdminNamespace = "Confluent.Kafka.Admin";

    private const string FutureToken = "FUTURE";

    private const BindingFlags PublicDeclared =
        BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    private static readonly Assembly s_assembly = typeof(IAdmin).Assembly;

    /// <summary>The members the async flavour exposes a <see cref="Task"/> on by design.</summary>
    private static readonly HashSet<string> s_taskExempt = new HashSet<string>(StringComparer.Ordinal)
    {
        nameof(IAsyncAdmin.Close),
        nameof(IAsyncDisposable.DisposeAsync),
    };

    /// <summary>The future types the async flavour must never expose: a <see cref="Task"/> or a sync future.</summary>
    private static readonly Type[] s_forbiddenOnAsync =
    {
        typeof(Task), typeof(Task<>), typeof(ValueTask), typeof(ValueTask<>), typeof(KafkaFuture), typeof(KafkaFuture<>),
    };

    /// <summary>The future types the sync flavour must never expose: a <see cref="Task"/> or an async future.</summary>
    private static readonly Type[] s_forbiddenOnSync =
    {
        typeof(Task), typeof(Task<>), typeof(ValueTask), typeof(ValueTask<>), typeof(AsyncKafkaFuture), typeof(AsyncKafkaFuture<>),
    };

    /// <summary>
    /// The sync Results converted to <see cref="KafkaFuture"/>s so far. Each M15/P14 S5a-f session adds its
    /// family's names; S5g replaces this set with every sync <c>*Result</c>.
    /// </summary>
    private static readonly string[] s_syncConvertedResults =
    {
        nameof(CreateTopicsResult),
        nameof(DeleteTopicsResult),
        nameof(DescribeTopicsResult),
        nameof(ListTopicsResult),
        nameof(CreatePartitionsResult),
        nameof(DeleteRecordsResult),
        nameof(DescribeClusterResult),
        nameof(ListConfigResourcesResult),
        nameof(DescribeConfigsResult),
        nameof(AlterConfigsResult),
        nameof(DescribeLogDirsResult),
        nameof(AlterReplicaLogDirsResult),
        nameof(DescribeReplicaLogDirsResult),
        nameof(ElectLeadersResult),
        nameof(AlterPartitionReassignmentsResult),
        nameof(ListPartitionReassignmentsResult),
        nameof(ListOffsetsResult),
    };

    /// <summary>
    /// The <see cref="IAdmin"/> RPCs converted to the sync Results so far, by name (every overload of a name is
    /// covered). Each M15/P14 S5a-f session adds its family's names; S5g replaces this set with every RPC.
    /// </summary>
    private static readonly string[] s_syncConvertedRpcs =
    {
        nameof(IAdmin.CreateTopics),
        nameof(IAdmin.DeleteTopics),
        nameof(IAdmin.DescribeTopics),
        nameof(IAdmin.ListTopics),
        nameof(IAdmin.CreatePartitions),
        nameof(IAdmin.DeleteRecords),
        nameof(IAdmin.DescribeCluster),
        nameof(IAdmin.ListConfigResources),
        nameof(IAdmin.DescribeConfigs),
        nameof(IAdmin.IncrementalAlterConfigs),
        nameof(IAdmin.DescribeLogDirs),
        nameof(IAdmin.AlterReplicaLogDirs),
        nameof(IAdmin.DescribeReplicaLogDirs),
        nameof(IAdmin.ElectLeaders),
        nameof(IAdmin.AlterPartitionReassignments),
        nameof(IAdmin.ListPartitionReassignments),
        nameof(IAdmin.ListOffsets),
    };

    private static readonly string[] s_mockOnly =
    {
        nameof(AsyncMockAdminClient.TimeoutNextRequest),
        nameof(AsyncMockAdminClient.UpdateBeginningOffsets),
        nameof(AsyncMockAdminClient.UpdateEndOffsets),
        nameof(AsyncMockAdminClient.UpdateConsumerGroupOffsets),
        nameof(AsyncMockAdminClient.SetFeatureLevels),
    };

    /// <summary>
    /// <see cref="IAsyncAdmin"/> declares the same 48 RPCs as <see cref="IAdmin"/> — names, overloads, parameter
    /// names, types and defaults — and each returns the <c>Async</c> twin of the Result its sync twin returns.
    /// </summary>
    [Fact]
    public void AsyncAdmin_DeclaresIAdminsRpcs_EachReturningTheAsyncTwinOfItsResult()
    {
        List<MethodInfo> sync = Rpcs(typeof(IAdmin));
        List<MethodInfo> async = Rpcs(typeof(IAsyncAdmin));

        Assert.Equal(48, sync.Count);
        Assert.Equal(
            sync.Select(RpcSignature).OrderBy(static s => s, StringComparer.Ordinal),
            async.Select(RpcSignature).OrderBy(static s => s, StringComparer.Ordinal));

        foreach (MethodInfo rpc in async)
        {
            MethodInfo twin = typeof(IAdmin).GetMethod(
                rpc.Name, rpc.GetParameters().Select(static p => p.ParameterType).ToArray())!;
            Assert.Equal("Async" + twin.ReturnType.Name, rpc.ReturnType.Name);
            Assert.Equal(AdminNamespace, rpc.ReturnType.Namespace);
        }
    }

    /// <summary>
    /// <c>Close(TimeSpan)</c> is a <see cref="Task"/> on both flavours — Java's <c>close(Duration)</c> blocks — and
    /// disposal comes from the two interfaces <see cref="IAsyncAdmin"/> extends, which are exactly those.
    /// </summary>
    [Fact]
    public void Close_IsATaskOnBothFlavours_AndAsyncAdminExtendsExactlyTheTwoDisposables()
    {
        Assert.Equal(typeof(Task), typeof(IAdmin).GetMethod(nameof(IAdmin.Close), new[] { typeof(TimeSpan) })!.ReturnType);
        Assert.Equal(
            typeof(Task), typeof(IAsyncAdmin).GetMethod(nameof(IAsyncAdmin.Close), new[] { typeof(TimeSpan) })!.ReturnType);

        Assert.Equal(
            new[] { typeof(IAsyncDisposable).FullName, typeof(IDisposable).FullName }.OrderBy(static n => n, StringComparer.Ordinal),
            typeof(IAsyncAdmin).GetInterfaces().Select(static i => i.FullName).OrderBy(static n => n, StringComparer.Ordinal));
    }

    /// <summary>
    /// There are exactly 44 <c>Async*Result</c> types, each paired with a sync twin, and each pair has the same
    /// public members with the same parameters — the types differing only in the flavour of their futures.
    /// </summary>
    [Fact]
    public void AsyncResults_AreExactly44_EachWithItsSyncTwinsMembers()
    {
        List<Type> asyncResults = AsyncResults();

        Assert.Equal(44, asyncResults.Count);

        foreach (Type asyncResult in asyncResults)
        {
            Type? twin = s_assembly.GetType(AdminNamespace + "." + asyncResult.Name.Substring("Async".Length));
            Assert.True(twin is not null, $"{asyncResult.Name} has no sync twin");
            Assert.True(twin!.IsPublic, $"{twin.Name} is not public");

            Assert.Equal(Members(twin), Members(asyncResult));
        }
    }

    /// <summary>
    /// No public member of the async flavour exposes a <see cref="Task"/>, a <see cref="ValueTask"/> or a sync
    /// <see cref="KafkaFuture"/> at any generic depth: its RPCs, its two clients and every <c>Async*Result</c> hand
    /// out <see cref="AsyncKafkaFuture"/>s only. <c>Close</c> and <c>DisposeAsync</c> are the stated exceptions.
    /// </summary>
    [Fact]
    public void AsyncFlavour_ExposesNoTaskNorSyncFuture_AtAnyDepth()
    {
        List<string> offenders = new List<string>();

        foreach (MethodInfo method in Rpcs(typeof(IAsyncAdmin)))
        {
            CheckSignature(method, s_forbiddenOnAsync, offenders);
        }

        foreach (Type client in new[] { typeof(AsyncKafkaAdminClient), typeof(AsyncMockAdminClient) })
        {
            foreach (MethodInfo method in client.GetMethods(PublicDeclared))
            {
                if (!s_taskExempt.Contains(method.Name))
                {
                    CheckSignature(method, s_forbiddenOnAsync, offenders);
                }
            }
        }

        foreach (Type result in AsyncResults())
        {
            CheckResult(result, s_forbiddenOnAsync, offenders);
        }

        Assert.Empty(offenders);
    }

    /// <summary>
    /// The sync flavour's converted members expose no <see cref="Task"/>, no <see cref="ValueTask"/> and no
    /// <see cref="AsyncKafkaFuture"/> at any generic depth: each converted Result's public members, and each
    /// converted RPC on <see cref="IAdmin"/> and on both sync clients, hand out <see cref="KafkaFuture"/>s only.
    /// The set is explicit (<see cref="s_syncConvertedResults"/>, <see cref="s_syncConvertedRpcs"/>) because the
    /// sync flavour is converted one family per session (M15/P14 S4-S5g); every name must resolve, so a misspelt
    /// or renamed entry cannot pass vacuously.
    /// </summary>
    [Fact]
    public void SyncFlavour_ConvertedMembers_ExposeNoTaskNorAsyncFuture_AtAnyDepth()
    {
        List<string> offenders = new List<string>();

        foreach (string name in s_syncConvertedResults)
        {
            Type? result = s_assembly.GetType(AdminNamespace + "." + name);
            Assert.True(result is not null && result.IsPublic, $"{name} is not a public admin type");
            CheckResult(result!, s_forbiddenOnSync, offenders);
        }

        foreach (Type admin in new[] { typeof(IAdmin), typeof(KafkaAdminClient), typeof(MockAdminClient) })
        {
            foreach (string name in s_syncConvertedRpcs)
            {
                MethodInfo[] overloads = admin.GetMethods(PublicDeclared).Where(m => m.Name == name).ToArray();
                Assert.True(overloads.Length > 0, $"{admin.Name}.{name} does not exist");

                foreach (MethodInfo method in overloads)
                {
                    CheckSignature(method, s_forbiddenOnSync, offenders);
                }
            }
        }

        Assert.Empty(offenders);
    }

    /// <summary>
    /// The four clients are sealed, the async two implement <see cref="IAsyncAdmin"/> only, and no
    /// <c>Async*Result</c> has a public constructor (follow-up FU-5).
    /// </summary>
    [Fact]
    public void Clients_AreSealed_AndAsyncResults_HaveNoPublicConstructor()
    {
        foreach (Type client in new[]
                 {
                     typeof(KafkaAdminClient), typeof(MockAdminClient),
                     typeof(AsyncKafkaAdminClient), typeof(AsyncMockAdminClient),
                 })
        {
            Assert.True(client.IsSealed, $"{client.Name} is not sealed");
        }

        foreach (Type client in new[] { typeof(AsyncKafkaAdminClient), typeof(AsyncMockAdminClient) })
        {
            Assert.True(typeof(IAsyncAdmin).IsAssignableFrom(client), $"{client.Name} is not an IAsyncAdmin");
            Assert.False(typeof(IAdmin).IsAssignableFrom(client), $"{client.Name} is an IAdmin");
        }

        foreach (Type result in AsyncResults())
        {
            Assert.Empty(result.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        }
    }

    /// <summary>
    /// The five mock-only seeding methods exist on both mocks with the same signature, synchronous on both.
    /// </summary>
    [Fact]
    public void MockOnlyMethods_ExistOnBothMocks_WithTheSameSignature()
    {
        foreach (string name in s_mockOnly)
        {
            MethodInfo sync = Assert.Single(typeof(MockAdminClient).GetMethods(PublicDeclared), m => m.Name == name);
            MethodInfo async = Assert.Single(typeof(AsyncMockAdminClient).GetMethods(PublicDeclared), m => m.Name == name);

            Assert.Equal(typeof(void), async.ReturnType);
            Assert.Equal(sync.ReturnType, async.ReturnType);
            Assert.Equal(
                sync.GetParameters().Select(static p => p.ParameterType),
                async.GetParameters().Select(static p => p.ParameterType));
        }
    }

    private static List<MethodInfo> Rpcs(Type admin) =>
        admin.GetMethods(PublicDeclared).Where(static m => m.Name != nameof(IAdmin.Close)).ToList();

    private static List<Type> AsyncResults() =>
        s_assembly.GetExportedTypes()
            .Where(static t => t.Namespace == AdminNamespace && t.DeclaringType is null
                && t.Name.StartsWith("Async", StringComparison.Ordinal)
                && t.Name.EndsWith("Result", StringComparison.Ordinal))
            .OrderBy(static t => t.Name, StringComparer.Ordinal)
            .ToList();

    private static string RpcSignature(MethodInfo method) =>
        method.Name + "(" + string.Join(", ", method.GetParameters().Select(static p =>
            Normalize(p.ParameterType) + " " + p.Name
            + (p.IsOptional ? " = " + (p.DefaultValue ?? "null") : string.Empty))) + ")";

    private static List<string> Members(Type type)
    {
        List<string> members = type.GetMethods(PublicDeclared)
            .Select(static m => (m.IsStatic ? "static " : string.Empty) + m.Name
                + "(" + string.Join(", ", m.GetParameters().Select(static p => Normalize(p.ParameterType) + " " + p.Name)) + ")"
                + " : " + Normalize(m.ReturnType))
            .Concat(type.GetProperties(PublicDeclared)
                .Select(static p => "property " + p.Name + " : " + Normalize(p.PropertyType)))
            .Concat(type.GetFields(PublicDeclared)
                .Select(static f => "field " + f.Name + " : " + Normalize(f.FieldType)))
            .ToList();
        members.Sort(StringComparer.Ordinal);
        return members;
    }

    /// <summary>
    /// A type's name with every future type — <see cref="Task"/>, <see cref="KafkaFuture"/>,
    /// <see cref="AsyncKafkaFuture"/>, and their generic forms — folded into one token, <see cref="Nullable{T}"/>
    /// unwrapped (an async future is a struct, so a nullable one is <c>AsyncKafkaFuture&lt;T&gt;?</c>), and an
    /// <c>Async*Result</c> named as its sync twin.
    /// </summary>
    private static string Normalize(Type type)
    {
        if (type.IsByRef)
        {
            return "ref " + Normalize(type.GetElementType()!);
        }

        if (type.IsArray)
        {
            return Normalize(type.GetElementType()!) + "[]";
        }

        if (type.IsGenericParameter)
        {
            return type.Name;
        }

        Type? underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null)
        {
            return Normalize(underlying);
        }

        if (type == typeof(Task) || type == typeof(KafkaFuture) || type == typeof(AsyncKafkaFuture))
        {
            return FutureToken;
        }

        if (type.IsGenericType)
        {
            Type definition = type.GetGenericTypeDefinition();
            string arguments = string.Join(", ", type.GetGenericArguments().Select(Normalize));
            if (definition == typeof(Task<>) || definition == typeof(KafkaFuture<>) || definition == typeof(AsyncKafkaFuture<>))
            {
                return FutureToken + "<" + arguments + ">";
            }

            string name = definition.FullName!;
            return name.Substring(0, name.IndexOf('`')) + "<" + arguments + ">";
        }

        if (type.Namespace == AdminNamespace && type.DeclaringType is null
            && type.Name.StartsWith("Async", StringComparison.Ordinal)
            && type.Name.EndsWith("Result", StringComparison.Ordinal))
        {
            return AdminNamespace + "." + type.Name.Substring("Async".Length);
        }

        return type.FullName!;
    }

    private static void CheckResult(Type result, Type[] forbidden, List<string> offenders)
    {
        foreach (MethodInfo method in result.GetMethods(PublicDeclared))
        {
            CheckSignature(method, forbidden, offenders);
        }

        foreach (ConstructorInfo constructor in result.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
        {
            foreach (ParameterInfo parameter in constructor.GetParameters())
            {
                Check(parameter.ParameterType, result.Name + ".ctor(" + parameter.Name + ")", forbidden, offenders);
            }
        }

        foreach (FieldInfo field in result.GetFields(PublicDeclared))
        {
            Check(field.FieldType, result.Name + "." + field.Name, forbidden, offenders);
        }

        // A public nested type is part of the Result's surface (Java nests some value types in their Result, e.g.
        // DescribeReplicaLogDirsResult.ReplicaLogDirInfo), so its members are walked by the same rules rather than
        // left unchecked.
        foreach (Type nested in result.GetNestedTypes(BindingFlags.Public))
        {
            CheckResult(nested, forbidden, offenders);
        }
    }

    private static void CheckSignature(MethodInfo method, Type[] forbidden, List<string> offenders)
    {
        string where = method.DeclaringType!.Name + "." + method.Name;
        Check(method.ReturnType, where + " returns", forbidden, offenders);
        foreach (ParameterInfo parameter in method.GetParameters())
        {
            Check(parameter.ParameterType, where + "(" + parameter.Name + ")", forbidden, offenders);
        }
    }

    private static void Check(Type type, string where, Type[] forbidden, List<string> offenders)
    {
        if (type.IsByRef || type.IsArray)
        {
            Check(type.GetElementType()!, where, forbidden, offenders);
            return;
        }

        if (type.IsGenericParameter)
        {
            return;
        }

        Type definition = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
        if (Array.IndexOf(forbidden, definition) >= 0)
        {
            offenders.Add(string.Format(CultureInfo.InvariantCulture, "{0}: {1}", where, type));
            return;
        }

        foreach (Type argument in type.GetGenericArguments())
        {
            Check(argument, where, forbidden, offenders);
        }
    }
}
