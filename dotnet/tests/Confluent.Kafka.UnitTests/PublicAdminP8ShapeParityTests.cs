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

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public shape of the six transaction and producer RPCs and their results against the Java classes they mirror.
/// </summary>
/// <remarks>
/// Five of the six results answer <c>All()</c> and the sixth <c>Result()</c>: Java's own asymmetry, pinned so that it
/// is not "fixed".
/// </remarks>
public sealed class PublicAdminP8ShapeParityTests
{
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

    [Theory]
    [InlineData(typeof(DescribeProducersResult), typeof(KafkaFuture<>))]
    [InlineData(typeof(AsyncDescribeProducersResult), typeof(AsyncKafkaFuture<>))]
    public void DescribeProducersResult_PublishesExactlyJavasTwoMembers(Type result, Type future)
    {
        Assert.Equal(new[] { "All", "PartitionResult" }, DeclaredPublicMethodNames(result));
        Assert.Empty(DeclaredPublicPropertyNames(result));

        Assert.Equal(
            future.MakeGenericType(typeof(DescribeProducersResult.PartitionProducerState)),
            result.GetMethod("PartitionResult")!.ReturnType);
        Assert.Equal(
            future.MakeGenericType(
                typeof(IReadOnlyDictionary<TopicPartition, DescribeProducersResult.PartitionProducerState>)),
            result.GetMethod("All")!.ReturnType);
    }

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

    [Theory]
    [InlineData(typeof(DescribeTransactionsResult), typeof(KafkaFuture<>))]
    [InlineData(typeof(AsyncDescribeTransactionsResult), typeof(AsyncKafkaFuture<>))]
    public void DescribeTransactionsResult_PublishesExactlyJavasTwoMembers(Type result, Type future)
    {
        Assert.Equal(new[] { "All", "Description" }, DeclaredPublicMethodNames(result));
        Assert.Empty(DeclaredPublicPropertyNames(result));

        Assert.Equal(
            future.MakeGenericType(typeof(TransactionDescription)),
            result.GetMethod("Description")!.ReturnType);
        Assert.Equal(
            future.MakeGenericType(typeof(IReadOnlyDictionary<string, TransactionDescription>)),
            result.GetMethod("All")!.ReturnType);
    }

    [Theory]
    [InlineData(typeof(IAdmin), typeof(ListTransactionsResult), typeof(KafkaFuture<>))]
    [InlineData(typeof(IAsyncAdmin), typeof(AsyncListTransactionsResult), typeof(AsyncKafkaFuture<>))]
    public void ListTransactions_TakesOnlyOptions_AndPublishesJavasThreeViews(Type client, Type result, Type future)
    {
        MethodInfo rpc = client.GetMethod(nameof(IAdmin.ListTransactions))!;
        Assert.Single(rpc.GetParameters());
        Assert.Equal(result, rpc.ReturnType);

        Assert.Equal(new[] { "All", "AllByBrokerId", "ByBrokerId" }, DeclaredPublicMethodNames(result));
        Assert.Empty(DeclaredPublicPropertyNames(result));

        // ByBrokerId is a future of a map of futures, of the same flavour at both levels: the nesting keeps a per-broker
        // failure addressable.
        Type listings = typeof(IReadOnlyCollection<TransactionListing>);
        Assert.Equal(
            future.MakeGenericType(
                typeof(IReadOnlyDictionary<,>).MakeGenericType(typeof(int), future.MakeGenericType(listings))),
            result.GetMethod("ByBrokerId")!.ReturnType);
        Assert.Equal(
            future.MakeGenericType(typeof(IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>)),
            result.GetMethod("AllByBrokerId")!.ReturnType);
        Assert.Equal(future.MakeGenericType(listings), result.GetMethod("All")!.ReturnType);
    }

    [Theory]
    [InlineData(typeof(FenceProducersResult), typeof(KafkaFuture), typeof(KafkaFuture<>))]
    [InlineData(typeof(AsyncFenceProducersResult), typeof(AsyncKafkaFuture), typeof(AsyncKafkaFuture<>))]
    public void FenceProducersResult_PublishesExactlyJavasFourMembers(Type result, Type voidFuture, Type future)
    {
        Assert.Equal(new[] { "All", "EpochId", "ProducerId" }, DeclaredPublicMethodNames(result));
        Assert.Equal(new[] { "FencedProducers" }, DeclaredPublicPropertyNames(result));

        // The two projections keep Java's widths: an epoch is a short here, although ProducerState.ProducerEpoch is an int.
        Assert.Equal(future.MakeGenericType(typeof(long)), result.GetMethod("ProducerId")!.ReturnType);
        Assert.Equal(future.MakeGenericType(typeof(short)), result.GetMethod("EpochId")!.ReturnType);
        Assert.Equal(voidFuture, result.GetMethod("All")!.ReturnType);

        // The erased view is payload-free, as Java's thenApply(p -> null) is.
        Assert.Equal(
            typeof(IReadOnlyDictionary<,>).MakeGenericType(typeof(string), voidFuture),
            result.GetProperty("FencedProducers")!.PropertyType);
    }

    [Fact]
    public void ResultHandleLessResults_PublishExactlyJavasSoleMember()
    {
        Assert.Equal(new[] { "All" }, DeclaredPublicMethodNames(typeof(AbortTransactionResult)));
        Assert.Equal(new[] { "Result" }, DeclaredPublicMethodNames(typeof(TerminateTransactionResult)));

        Assert.Empty(DeclaredPublicPropertyNames(typeof(AbortTransactionResult)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(TerminateTransactionResult)));
    }

    [Theory]
    [InlineData(typeof(AbortTransactionResult), "All", typeof(KafkaFuture))]
    [InlineData(typeof(TerminateTransactionResult), "Result", typeof(KafkaFuture))]
    [InlineData(typeof(AsyncAbortTransactionResult), "All", typeof(AsyncKafkaFuture))]
    [InlineData(typeof(AsyncTerminateTransactionResult), "Result", typeof(AsyncKafkaFuture))]
    public void ResultHandleLessResults_ReturnAPayloadFreeFuture(Type result, string member, Type future)
    {
        Assert.Equal(future, result.GetMethod(member)!.ReturnType);
    }

    private static string[] DeclaredPublicMethodNames(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method =>
                !method.IsSpecialName
                && !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
                && method.GetBaseDefinition().DeclaringType != typeof(object))
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    private static string[] DeclaredPublicPropertyNames(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(property => !property.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            .Select(property => property.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
}
