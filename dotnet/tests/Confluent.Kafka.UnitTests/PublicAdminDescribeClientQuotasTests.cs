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
using Confluent.Kafka.Common.Quota;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DescribeClientQuotas</c> end to end against the mocks, whose one future fails with the mock's own answer, as
/// Java's mock does: the result has <c>Entities</c> and no <c>All</c>, every match type reaches the core, the
/// preconditions, the result type and the options.
/// </summary>
public sealed class PublicAdminDescribeClientQuotasTests
{
    private const string NotImplemented = "Not implement yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Surface_IsEntitiesOnly_WithNoAll()
    {
        MethodInfo[] declared = typeof(DescribeClientQuotasResult)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .ToArray();

        // A control: the reflection sees this type's own methods.
        Assert.Contains(
            declared, method => method.Name == nameof(DescribeClientQuotasResult.Entities));
        Assert.DoesNotContain(declared, method => method.Name == "All");

        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>),
            typeof(DescribeClientQuotasResult)
                .GetMethod(nameof(DescribeClientQuotasResult.Entities))!
                .ReturnType);
        Assert.Equal(
            typeof(AsyncKafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>),
            typeof(AsyncDescribeClientQuotasResult)
                .GetMethod(nameof(AsyncDescribeClientQuotasResult.Entities))!
                .ReturnType);
    }

    [Fact]
    public async Task WholeCallFailure_FaultsTheSingleTask()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClientQuotasResult result = admin.DescribeClientQuotas(ClientQuotaFilter.All());

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task Entities_IsTheSameTaskEveryCall()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClientQuotasResult result = admin.DescribeClientQuotas(ClientQuotaFilter.All());
        Assert.Same(result.Entities().Get(), result.Entities().Get());

        await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
    }

    [Theory]
    [InlineData(ClientQuotaMatchType.Exact)]
    [InlineData(ClientQuotaMatchType.Default)]
    [InlineData(ClientQuotaMatchType.Specified)]
    public async Task EveryMatchType_ReachesTheCore(ClientQuotaMatchType matchType)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ClientQuotaFilterComponent component = matchType switch
        {
            ClientQuotaMatchType.Exact =>
                ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice"),
            ClientQuotaMatchType.Default =>
                ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User),
            _ => ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User),
        };

        AsyncDescribeClientQuotasResult result = admin.DescribeClientQuotas(
            ClientQuotaFilter.ContainsOnly(new[] { component }));

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "filter",
            Assert.Throws<ArgumentNullException>(
                () => admin.DescribeClientQuotas(null!)).ParamName);

        // A negative timeout is not a precondition: it is sent as 0.
        Assert.Null(Record.Exception(
            () => admin.DescribeClientQuotas(
                ClientQuotaFilter.All(), new DescribeClientQuotasOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeClientQuotas(ClientQuotaFilter.All()));
    }

    [Fact]
    public void Constructor_IsInternalAndRejectsDefault()
    {
        Assert.Empty(typeof(DescribeClientQuotasResult).GetConstructors());
        ConstructorInfo constructor = Assert.Single(
            typeof(DescribeClientQuotasResult).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance),
            candidate => candidate.IsAssembly);
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>),
            Assert.Single(constructor.GetParameters()).ParameterType);

        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => new DescribeClientQuotasResult(default));
        Assert.Equal("entities", error.ParamName);
        Assert.Equal(new ArgumentNullException("entities").Message, error.Message);
    }

    [Fact]
    public void Options_DefaultToJavasDefaults() =>
        Assert.Null(new DescribeClientQuotasOptions().TimeoutMs);
}
