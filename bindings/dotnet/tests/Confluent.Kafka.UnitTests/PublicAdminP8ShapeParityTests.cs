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
/// Pins the <b>public shape</b> of M15/P8's surface — the six RPCs and their results — against
/// the Java classes it mirrors.
/// </summary>
/// <remarks>
/// ⚠⚠ <b>Five of the six results answer <c>All()</c> and the sixth answers <c>Result()</c>.</b>
/// That is Java's own asymmetry (<c>TerminateTransactionResult.java:36</c>) and is pinned here
/// because a C# caller would reasonably "fix" it, and nothing else would notice.
/// </remarks>
public sealed class PublicAdminP8ShapeParityTests
{
    /// <summary>
    /// Each RPC returns its <c>*Result</c> <b>synchronously</b> with Java's parameter shape
    /// (<c>admin-client.md</c> §1), ending in an optional options parameter.
    /// </summary>
    /// <param name="name">The RPC's name on <see cref="IAdmin"/>.</param>
    /// <param name="resultType">The result type Java declares for it.</param>
    [Theory]
    [InlineData(nameof(IAdmin.AbortTransaction), typeof(AbortTransactionResult))]
    [InlineData(nameof(IAdmin.ForceTerminateTransaction), typeof(TerminateTransactionResult))]
    [InlineData(nameof(IAdmin.FenceProducers), typeof(FenceProducersResult))]
    [InlineData(nameof(IAdmin.DescribeTransactions), typeof(DescribeTransactionsResult))]
    [InlineData(nameof(IAdmin.DescribeProducers), typeof(DescribeProducersResult))]
    [InlineData(nameof(IAdmin.ListTransactions), typeof(ListTransactionsResult))]
    public void EachRpc_IsSynchronous_AndEndsInAnOptionalOptions(string name, Type resultType)
    {
        MethodInfo rpc = typeof(IAdmin).GetMethod(name)!;

        Assert.Equal(resultType, rpc.ReturnType);

        ParameterInfo[] parameters = rpc.GetParameters();
        ParameterInfo options = parameters[parameters.Length - 1];
        Assert.EndsWith("Options", options.ParameterType.Name, StringComparison.Ordinal);
        Assert.True(options.IsOptional, $"{name}'s options must be optional");
    }

    /// <summary>Each RPC's required parameters mirror Java's.</summary>
    [Fact]
    public void EachRpc_TakesJavasOwnRequiredParameters()
    {
        Assert.Equal(
            typeof(AbortTransactionSpec),
            typeof(IAdmin).GetMethod(nameof(IAdmin.AbortTransaction))!.GetParameters()[0].ParameterType);

        Assert.Equal(
            typeof(string),
            typeof(IAdmin).GetMethod(nameof(IAdmin.ForceTerminateTransaction))!
                .GetParameters()[0].ParameterType);

        // Both id-batch RPCs take Java's Collection<String> as the read-only collection this
        // binding maps it to, not a single id.
        Assert.Equal(
            typeof(IReadOnlyCollection<string>),
            typeof(IAdmin).GetMethod(nameof(IAdmin.FenceProducers))!.GetParameters()[0].ParameterType);
        Assert.Equal(
            typeof(IReadOnlyCollection<string>),
            typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeTransactions))!
                .GetParameters()[0].ParameterType);

        Assert.Equal(
            typeof(IReadOnlyCollection<TopicPartition>),
            typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeProducers))!
                .GetParameters()[0].ParameterType);
    }

    /// <summary>
    /// ⚠ <see cref="DescribeProducersResult"/> publishes <b>no per-partition map</b> either:
    /// Java declares only <c>partitionResult(TopicPartition)</c> and <c>all()</c>
    /// (<c>DescribeProducersResult.java:36, :44</c>).
    /// </summary>
    [Fact]
    public void DescribeProducersResult_PublishesExactlyJavasTwoMembers()
    {
        Assert.Equal(
            new[] { "All", "PartitionResult" },
            DeclaredPublicMethodNames(typeof(DescribeProducersResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(DescribeProducersResult)));

        Assert.Equal(
            typeof(Task<DescribeProducersResult.PartitionProducerState>),
            typeof(DescribeProducersResult).GetMethod("PartitionResult")!.ReturnType);
    }

    /// <summary>
    /// ⚠ <c>PartitionProducerState</c> is <b>nested</b> inside its result, as Java nests it
    /// (<c>DescribeProducersResult.java:61</c>), and publishes exactly its one accessor.
    /// </summary>
    [Fact]
    public void PartitionProducerState_IsNestedAndPublishesJavasSoleAccessor()
    {
        Assert.Equal(
            typeof(DescribeProducersResult),
            typeof(DescribeProducersResult.PartitionProducerState).DeclaringType);

        Assert.Empty(DeclaredPublicMethodNames(typeof(DescribeProducersResult.PartitionProducerState)));
        Assert.Equal(
            new[] { "ActiveProducers" },
            DeclaredPublicPropertyNames(typeof(DescribeProducersResult.PartitionProducerState)));
    }

    /// <summary>
    /// ⚠ <see cref="DescribeTransactionsResult"/> publishes <b>no per-id map</b>: Java declares
    /// only <c>description(String)</c> and <c>all()</c>
    /// (<c>DescribeTransactionsResult.java:43, :62</c>), unlike every sibling describe result.
    /// </summary>
    [Fact]
    public void DescribeTransactionsResult_PublishesExactlyJavasTwoMembers()
    {
        Assert.Equal(
            new[] { "All", "Description" },
            DeclaredPublicMethodNames(typeof(DescribeTransactionsResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(DescribeTransactionsResult)));

        Assert.Equal(
            typeof(Task<TransactionDescription>),
            typeof(DescribeTransactionsResult).GetMethod("Description")!.ReturnType);
    }

    /// <summary>
    /// ⚠ <c>listTransactions</c> is the one P8 RPC taking <b>only</b> options — Java has no
    /// required parameter for it — and its result publishes exactly Java's three views
    /// (<c>ListTransactionsResult.java:48, :68, :92</c>).
    /// </summary>
    [Fact]
    public void ListTransactions_TakesOnlyOptions_AndPublishesJavasThreeViews()
    {
        Assert.Single(typeof(IAdmin).GetMethod(nameof(IAdmin.ListTransactions))!.GetParameters());

        Assert.Equal(
            new[] { "All", "AllByBrokerId", "ByBrokerId" },
            DeclaredPublicMethodNames(typeof(ListTransactionsResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(ListTransactionsResult)));

        // ⚠ byBrokerId() is a future OF a map OF futures: the nesting is what keeps a
        // per-broker failure addressable, and flattening it would silently drop that view's
        // whole purpose.
        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>),
            typeof(ListTransactionsResult).GetMethod("ByBrokerId")!.ReturnType);
        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>),
            typeof(ListTransactionsResult).GetMethod("AllByBrokerId")!.ReturnType);
        Assert.Equal(
            typeof(Task<IReadOnlyCollection<TransactionListing>>),
            typeof(ListTransactionsResult).GetMethod("All")!.ReturnType);
    }

    /// <summary>
    /// <see cref="FenceProducersResult"/> publishes Java's erased map plus the two projections
    /// (<c>FenceProducersResult.java:44, :53, :60, :67</c>) — and no <c>ProducerIdAndEpoch</c>
    /// (M15/P8 D50).
    /// </summary>
    [Fact]
    public void FenceProducersResult_PublishesExactlyJavasFourMembers()
    {
        Assert.Equal(
            new[] { "All", "EpochId", "ProducerId" },
            DeclaredPublicMethodNames(typeof(FenceProducersResult)));
        Assert.Equal(
            new[] { "FencedProducers" },
            DeclaredPublicPropertyNames(typeof(FenceProducersResult)));

        // ⚠ The two projections keep Java's own widths: an epoch is a short here although
        // ProducerState.ProducerEpoch beside it is an int. A C# widening would be silent.
        Assert.Equal(
            typeof(Task<long>),
            typeof(FenceProducersResult).GetMethod("ProducerId")!.ReturnType);
        Assert.Equal(
            typeof(Task<short>),
            typeof(FenceProducersResult).GetMethod("EpochId")!.ReturnType);

        // The erased view is payload-free, mirroring Java's thenApply(p -> null).
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, Task>),
            typeof(FenceProducersResult).GetProperty("FencedProducers")!.PropertyType);
    }

    /// <summary>
    /// ⚠ <see cref="TerminateTransactionResult"/>'s sole member is <c>Result</c>, not
    /// <c>All</c> — Java's <c>result()</c>. Both results publish exactly one method and no
    /// properties, because the ABI exposes no result handle for either RPC.
    /// </summary>
    [Fact]
    public void ResultHandleLessResults_PublishExactlyJavasSoleMember()
    {
        Assert.Equal(new[] { "All" }, DeclaredPublicMethodNames(typeof(AbortTransactionResult)));
        Assert.Equal(new[] { "Result" }, DeclaredPublicMethodNames(typeof(TerminateTransactionResult)));

        Assert.Empty(DeclaredPublicPropertyNames(typeof(AbortTransactionResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(TerminateTransactionResult)));
    }

    /// <summary>
    /// Both members return the non-generic <see cref="Task"/> — Java's
    /// <c>KafkaFuture&lt;Void&gt;</c> carries no payload, so a <c>Task&lt;T&gt;</c> here would
    /// be inventing one.
    /// </summary>
    [Fact]
    public void ResultHandleLessResults_ReturnAPayloadFreeTask()
    {
        Assert.Equal(typeof(Task), typeof(AbortTransactionResult).GetMethod("All")!.ReturnType);
        Assert.Equal(typeof(Task), typeof(TerminateTransactionResult).GetMethod("Result")!.ReturnType);
    }

    /// <summary>
    /// The type's own public instance methods, by name — selected by
    /// <see cref="BindingFlags"/> plus <see cref="CompilerGeneratedAttribute"/> and
    /// <see cref="MethodBase.IsSpecialName"/>, never by a name predicate.
    /// </summary>
    /// <param name="type">The type to inspect.</param>
    /// <returns>The sorted method names.</returns>
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
}
