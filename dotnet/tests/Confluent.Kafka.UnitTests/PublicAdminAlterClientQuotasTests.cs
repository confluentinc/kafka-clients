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
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Quota;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>AlterClientQuotas</c> end to end against the mocks, which fail every entity with the mock's own answer, as Java's
/// mock does: per-entity results keyed by entity value, <c>All()</c>, the preconditions, the result type and the options.
/// </summary>
public sealed class PublicAdminAlterClientQuotasTests
{
    private const string NotImplemented = "Not implement yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly ClientQuotaAlteration.Op[] s_noOps =
        Array.Empty<ClientQuotaAlteration.Op>();

    [Fact]
    public async Task AnEmptyEntity_IsSent_AndAnsweredForItselfAlone()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ClientQuotaEntity empty = EmptyEntity();
        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("valid", 1d), new ClientQuotaAlteration(empty, s_noOps) });

        Assert.Equal(2, result.Values.Count);
        Assert.True(result.Values.ContainsKey(Entity("valid")));
        Assert.True(result.Values.ContainsKey(EmptyEntity()));

        foreach (AsyncKafkaFuture perEntity in new[] { result.Values[Entity("valid")], result.Values[empty] })
        {
            NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
                () => TestTimeout.Run(() => perEntity.Get(), s_deadline));

            // The core mock's own per-entity wording, typo included, not the whole-call rejection.
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    [Fact]
    public async Task TwoEmptyEntities_AreOneEntity_SentAndAnsweredOnce()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[]
            {
                new ClientQuotaAlteration(EmptyEntity(), s_noOps),
                new ClientQuotaAlteration(EmptyEntity(), s_noOps),
            });

        Assert.Equal(EmptyEntity(), Assert.Single(result.Values).Key);

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[EmptyEntity()].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task ARepeatedEntity_IsAccepted_AndHasOneAwaitable()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("dup", 1d), Alteration("dup", 2d) });

        Assert.Equal(Entity("dup"), Assert.Single(result.Values).Key);

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[Entity("dup")].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);

        NotSupportedException all = await Assert.ThrowsAsync<NotSupportedException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
        Assert.Equal(NotImplemented, all.Message);
    }

    [Fact]
    public async Task Values_AreKeyedByValue_NotByReference()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("keyed-by-value", 1d) });

        ClientQuotaEntity lookup = Entity("keyed-by-value");
        Assert.NotSame(lookup, Assert.Single(result.Values).Key);
        Assert.True(result.Values.ContainsKey(lookup));

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[lookup].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task TheEntityNameTernary_ProducesThreeDistinctKeys()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[]
            {
                new ClientQuotaAlteration(Entity(null), s_noOps),
                new ClientQuotaAlteration(Entity(string.Empty), s_noOps),
                new ClientQuotaAlteration(Entity("x"), s_noOps),
            });

        Assert.Equal(3, result.Values.Count);
        Assert.True(result.Values.ContainsKey(Entity(null)));
        Assert.True(result.Values.ContainsKey(Entity(string.Empty)));
        Assert.True(result.Values.ContainsKey(Entity("x")));

        Assert.NotEqual(Entity(null), Entity(string.Empty));

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task EachEntity_GetsItsOwnTask_AndItsKeyRoundTrips()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ClientQuotaEntity alpha = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [ClientQuotaEntity.User] = "alice",
                [ClientQuotaEntity.ClientId] = "app-1",
            });
        ClientQuotaEntity beta = Entity("bob");

        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[]
            {
                new ClientQuotaAlteration(
                    alpha, new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1024d) }),
                new ClientQuotaAlteration(
                    beta,
                    new[]
                    {
                        new ClientQuotaAlteration.Op("consumer_byte_rate", null),
                        new ClientQuotaAlteration.Op("request_percentage", 0d),
                    }),
            });

        Assert.Equal(2, result.Values.Count);
        Assert.NotSame(result.Values[alpha].Get(), result.Values[beta].Get());
        Assert.Contains(alpha, result.Values.Keys);
        Assert.Contains(beta, result.Values.Keys);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[alpha].Get(), s_deadline));
        await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[beta].Get(), s_deadline));
    }

    [Fact]
    public async Task OneKey_IsAwaitableWithoutTheOthers()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ClientQuotaEntity first = Entity("solo-first");
        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("solo-first", 1d), Alteration("solo-second", 2d) });

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[first].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);

        // Observe the other one too, so no task is left with an unobserved fault.
        await Assert.ThrowsAsync<NotSupportedException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task All_FaultsWhenAnyEntityFails()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("all-a", 1d), Alteration("all-b", 2d) });

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task All_OverAnEmptyRequest_Completes()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result =
            admin.AlterClientQuotas(Array.Empty<ClientQuotaAlteration>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task NonAsciiName_RoundTripsThroughTheResultKey()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result =
            admin.AlterClientQuotas(new[] { Alteration("用户-café", 1d) });

        ClientQuotaEntity key = Assert.Single(result.Values).Key;
        Assert.Equal("用户-café", key.Entries[ClientQuotaEntity.User]);
        Assert.Equal(Entity("用户-café"), key);

        await Assert.ThrowsAsync<NotSupportedException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "entries",
            Assert.Throws<ArgumentNullException>(() => admin.AlterClientQuotas(null!)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => admin.AlterClientQuotas(
                new ClientQuotaAlteration?[] { Alteration("ok", 1d), null }!));
        Assert.Equal("entries", nullElement.ParamName);
        Assert.StartsWith(
            "The client quota alterations must not contain a null element.",
            nullElement.Message,
            StringComparison.Ordinal);

        // A negative timeout is not a precondition: it is sent as 0, as Java clamps the deadline.
        Assert.Null(Record.Exception(
            () => admin.AlterClientQuotas(
                new[] { Alteration("ok", 1d) }, new AlterClientQuotasOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void TheValueTypes_RejectTheRemainingAbiInputs()
    {
        Assert.Equal(
            "entity",
            Assert.Throws<ArgumentNullException>(
                () => new ClientQuotaAlteration(null!, s_noOps)).ParamName);

        Assert.Equal(
            "key",
            Assert.Throws<ArgumentNullException>(
                () => new ClientQuotaAlteration.Op(null!, 1d)).ParamName);

        ArgumentException nullOp = Assert.Throws<ArgumentException>(
            () => new ClientQuotaAlteration(
                Entity("ops"), new ClientQuotaAlteration.Op?[] { null }!));
        Assert.Equal("ops", nullOp.ParamName);
    }

    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(
            () => admin.AlterClientQuotas(new[] { Alteration("gone", 1d) }));
    }

    [Fact]
    public void Constructor_IsInternalAndRejectsNull()
    {
        Assert.Empty(typeof(AlterClientQuotasResult).GetConstructors());
        ConstructorInfo constructor = Assert.Single(
            typeof(AlterClientQuotasResult).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance),
            candidate => candidate.IsAssembly);
        Assert.Equal(
            typeof(IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture>),
            Assert.Single(constructor.GetParameters()).ParameterType);

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(() => new AlterClientQuotasResult(null!));
        Assert.Equal("futures", error.ParamName);
        Assert.Equal(new ArgumentNullException("futures").Message, error.Message);
    }

    [Fact]
    public void Options_DefaultToJavasDefaults()
    {
        AlterClientQuotasOptions options = new AlterClientQuotasOptions();

        Assert.Null(options.TimeoutMs);
        Assert.False(options.ValidateOnly);
    }

    private static ClientQuotaEntity Entity(string? user) =>
        new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [ClientQuotaEntity.User] = user,
            });

    private static ClientQuotaEntity EmptyEntity() =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal));

    private static ClientQuotaAlteration Alteration(string user, double value) =>
        new ClientQuotaAlteration(
            Entity(user), new[] { new ClientQuotaAlteration.Op("producer_byte_rate", value) });
}
