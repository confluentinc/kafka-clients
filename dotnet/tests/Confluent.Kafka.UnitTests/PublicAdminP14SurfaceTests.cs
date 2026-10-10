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
/// Pins the <b>public shape</b> of M15/P14's two admin flavours — the sync <see cref="IAdmin"/> and the async
/// <see cref="IAsyncAdmin"/>, their four clients and their 44 + 44 <c>*Result</c> types — against each other.
/// </summary>
/// <remarks>
/// The flavours must offer the same RPCs and the same Result members, differing only in the future type at the
/// public boundary. These tests compare the two by reflection with every future type folded into one
/// flavour-neutral token, so a member added to, dropped from or re-typed on one flavour only turns them red; and
/// each flavour's whole surface is swept for the other flavour's future types (and for <see cref="Task"/>).
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
    /// <c>Close(TimeSpan)</c> — Java's <c>close(Duration)</c>, which blocks — is <c>void</c> on the sync flavour and a
    /// <see cref="Task"/> on the async one, and each is the only member besides the 48 RPCs. Disposal comes from the
    /// interfaces each extends, which are exactly <see cref="IDisposable"/> for <see cref="IAdmin"/> and
    /// <see cref="IAsyncDisposable"/> plus <see cref="IDisposable"/> for <see cref="IAsyncAdmin"/>.
    /// </summary>
    [Fact]
    public void Close_IsVoidOnTheSyncFlavourAndATaskOnTheAsync_AndEachInterfaceExtendsExactlyItsDisposables()
    {
        Assert.Equal(typeof(void), typeof(IAdmin).GetMethod(nameof(IAdmin.Close), new[] { typeof(TimeSpan) })!.ReturnType);
        Assert.Equal(
            typeof(Task), typeof(IAsyncAdmin).GetMethod(nameof(IAsyncAdmin.Close), new[] { typeof(TimeSpan) })!.ReturnType);

        Assert.Equal(49, typeof(IAdmin).GetMethods(PublicDeclared).Length);
        Assert.Equal(49, typeof(IAsyncAdmin).GetMethods(PublicDeclared).Length);

        Assert.Equal(new[] { typeof(IDisposable).FullName }, InterfaceNames(typeof(IAdmin)));
        Assert.Equal(
            new[] { typeof(IAsyncDisposable).FullName, typeof(IDisposable).FullName }.OrderBy(static n => n, StringComparer.Ordinal),
            InterfaceNames(typeof(IAsyncAdmin)));
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
    /// No public member of the sync flavour exposes a <see cref="Task"/>, a <see cref="ValueTask"/> or an
    /// <see cref="AsyncKafkaFuture"/> at any generic depth, with no exemption: every member <see cref="IAdmin"/>
    /// declares (<c>Close</c> included), every public method and constructor of its two clients, and every sync
    /// <c>*Result</c> — found by reflection, so a Result added later is covered without editing a list, and pinned
    /// at 44 with the same names as the async twins, so the sweep cannot pass by finding nothing.
    /// </summary>
    [Fact]
    public void SyncFlavour_ExposesNoTaskNorAsyncFuture_AtAnyDepth()
    {
        List<string> offenders = new List<string>();

        foreach (MethodInfo method in typeof(IAdmin).GetMethods(PublicDeclared))
        {
            CheckSignature(method, s_forbiddenOnSync, offenders);
        }

        foreach (Type client in new[] { typeof(KafkaAdminClient), typeof(MockAdminClient) })
        {
            foreach (MethodInfo method in client.GetMethods(PublicDeclared))
            {
                CheckSignature(method, s_forbiddenOnSync, offenders);
            }

            foreach (ConstructorInfo constructor in client.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
            {
                foreach (ParameterInfo parameter in constructor.GetParameters())
                {
                    Check(parameter.ParameterType, client.Name + ".ctor(" + parameter.Name + ")", s_forbiddenOnSync, offenders);
                }
            }
        }

        List<Type> syncResults = SyncResults();
        Assert.Equal(44, syncResults.Count);
        Assert.Equal(
            AsyncResults().Select(static t => t.Name.Substring("Async".Length)),
            syncResults.Select(static t => t.Name));

        foreach (Type result in syncResults)
        {
            CheckResult(result, s_forbiddenOnSync, offenders);
        }

        Assert.Empty(offenders);
    }

    /// <summary>
    /// The four clients are sealed and implement exactly their own flavour's interfaces — the sync two
    /// <see cref="IAdmin"/> and <see cref="IDisposable"/> (so no <see cref="IAsyncDisposable"/>, not even explicitly),
    /// the async two <see cref="IAsyncAdmin"/>, <see cref="IAsyncDisposable"/> and <see cref="IDisposable"/> — and no
    /// <c>*Result</c> of either flavour has a public constructor (Q3 (b), follow-up FU-5).
    /// </summary>
    [Fact]
    public void Clients_AreSealed_ImplementOnlyTheirFlavour_AndNoResultHasAPublicConstructor()
    {
        foreach (Type client in new[]
                 {
                     typeof(KafkaAdminClient), typeof(MockAdminClient),
                     typeof(AsyncKafkaAdminClient), typeof(AsyncMockAdminClient),
                 })
        {
            Assert.True(client.IsSealed, $"{client.Name} is not sealed");
        }

        foreach (Type client in new[] { typeof(KafkaAdminClient), typeof(MockAdminClient) })
        {
            Assert.Equal(
                new[] { typeof(IAdmin).FullName, typeof(IDisposable).FullName }.OrderBy(static n => n, StringComparer.Ordinal),
                InterfaceNames(client));
            Assert.False(typeof(IAsyncAdmin).IsAssignableFrom(client), $"{client.Name} is an IAsyncAdmin");
            Assert.False(typeof(IAsyncDisposable).IsAssignableFrom(client), $"{client.Name} is an IAsyncDisposable");
        }

        foreach (Type client in new[] { typeof(AsyncKafkaAdminClient), typeof(AsyncMockAdminClient) })
        {
            Assert.Equal(
                new[] { typeof(IAsyncAdmin).FullName, typeof(IAsyncDisposable).FullName, typeof(IDisposable).FullName }
                    .OrderBy(static n => n, StringComparer.Ordinal),
                InterfaceNames(client));
            Assert.False(typeof(IAdmin).IsAssignableFrom(client), $"{client.Name} is an IAdmin");
        }

        foreach (Type result in AsyncResults().Concat(SyncResults()))
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

    private static List<Type> SyncResults() =>
        s_assembly.GetExportedTypes()
            .Where(static t => t.Namespace == AdminNamespace && t.DeclaringType is null
                && !t.Name.StartsWith("Async", StringComparison.Ordinal)
                && t.Name.EndsWith("Result", StringComparison.Ordinal))
            .OrderBy(static t => t.Name, StringComparer.Ordinal)
            .ToList();

    private static IEnumerable<string?> InterfaceNames(Type type) =>
        type.GetInterfaces().Select(static i => i.FullName).OrderBy(static n => n, StringComparer.Ordinal);

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
