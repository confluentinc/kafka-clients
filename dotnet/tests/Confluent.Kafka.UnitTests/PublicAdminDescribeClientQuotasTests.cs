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
/// <c>describeClientQuotas</c> end to end against <see cref="MockAdminClient"/>, no broker
/// (M15/P6).
/// </summary>
/// <remarks>
/// Java's <c>MockAdminClient.describeClientQuotas</c> throws
/// <c>UnsupportedOperationException("Not implement yet")</c> — Java's own typo, mirrored
/// (<c>MockAdminClient.java:1244-1246</c>) — and the Rust mock surfaces it as one exceptional
/// future for the whole call. Because the result carries no per-entity error channel, that
/// failure faults the single task, which is the contract asserted here; the entity and quota
/// readers are covered by injection in <c>AdminP6ResultMarshalTests</c>.
/// </remarks>
public sealed class PublicAdminDescribeClientQuotasTests
{
    private const string NotImplemented = "Not implement yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The type publishes <c>Entities()</c> and <b>no</b> <c>All()</c> — Java declares none
    /// (<c>DescribeClientQuotasResult.java:46</c>).
    /// </summary>
    [Fact]
    public void Surface_IsEntitiesOnly_WithNoAll()
    {
        MethodInfo[] declared = typeof(DescribeClientQuotasResult)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .ToArray();

        // Control-positive: the reflection really sees this type's own methods.
        Assert.Contains(
            declared, method => method.Name == nameof(DescribeClientQuotasResult.Entities));
        Assert.DoesNotContain(declared, method => method.Name == "All");

        // M15/P14 S5d: the sync flavour hands out a KafkaFuture, its async twin an AsyncKafkaFuture.
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

    /// <summary>
    /// A whole-call failure faults the <b>single</b> task, with the message and code the core
    /// reported.
    /// </summary>
    [Fact]
    public async Task WholeCallFailure_FaultsTheSingleTask()
    {
        AsyncMockAdminClient admin = new AsyncMockAdminClient(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClientQuotasResult result = admin.DescribeClientQuotas(ClientQuotaFilter.All());

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
        Assert.NotEqual(0, failure.Code);
    }

    /// <summary>
    /// <c>Entities()</c> hands back the <b>same</b> task instance every call, so a fault can
    /// never go unobserved on a second projection.
    /// </summary>
    [Fact]
    public async Task Entities_IsTheSameTaskEveryCall()
    {
        AsyncMockAdminClient admin = new AsyncMockAdminClient(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClientQuotasResult result = admin.DescribeClientQuotas(ClientQuotaFilter.All());
        Assert.Same(result.Entities().Get(), result.Entities().Get());

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
    }

    /// <summary>
    /// All three component factories reach the core rather than being screened out — no enum
    /// combination is rejected binding-side.
    /// </summary>
    [Theory]
    [InlineData(ClientQuotaMatchType.Exact)]
    [InlineData(ClientQuotaMatchType.Default)]
    [InlineData(ClientQuotaMatchType.Specified)]
    public async Task EveryMatchType_ReachesTheCore(ClientQuotaMatchType matchType)
    {
        AsyncMockAdminClient admin = new AsyncMockAdminClient(1);
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

        // It reached the core and came back as a call-level failure, not an argument error.
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// Preconditions are .NET exceptions raised before any native call (ffi §B5).
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        MockAdminClient admin = new MockAdminClient(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "filter",
            Assert.Throws<ArgumentNullException>(
                () => admin.DescribeClientQuotas(null!)).ParamName);

        // A negative timeout is not a precondition (M15/P13.5 X2): it is sent as 0.
        Assert.Null(Record.Exception(
            () => admin.DescribeClientQuotas(
                ClientQuotaFilter.All(), new DescribeClientQuotasOptions { TimeoutMs = -1 })));
    }

    /// <summary>A call after <c>Dispose</c> throws <see cref="ObjectDisposedException"/>.</summary>
    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient(1);
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(
            () => admin.DescribeClientQuotas(ClientQuotaFilter.All()));
    }

    /// <summary>
    /// Java's constructor is <b>public</b> (<c>DescribeClientQuotasResult.java:39</c>); the sync one here is
    /// <b>internal</b> — a recorded Java-parity deviation, follow-up FU-5 (M15/P14 Q3) — over one
    /// <see cref="KafkaFuture{T}"/>, and it still rejects a default future, bound to no source, before anything can
    /// read it (reached through <c>InternalsVisibleTo</c>).
    /// </summary>
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

    /// <summary>The options default to Java's defaults — an unset timeout.</summary>
    [Fact]
    public void Options_DefaultToJavasDefaults() =>
        Assert.Null(new DescribeClientQuotasOptions().TimeoutMs);
}
