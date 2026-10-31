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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <c>alterClientQuotas</c> with the same entity in more than one alteration: every alteration is sent, and the
/// core's per-entity map makes the entity one key, answered here with the core mock's failure.
/// </summary>
public sealed class AdminAlterClientQuotasRepeatedEntityTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implement yet";

    [Fact]
    public void ARepeatedEntity_IsKeptForEveryAlteration_InOrder()
    {
        ClientQuotaAlteration first = Alteration("dup", 1d);
        ClientQuotaAlteration second = Alteration("dup", 2d);

        List<ClientQuotaAlteration> sent = NativeAdminClient.RequestedAlterations(new[] { first, second }, "entries");

        Assert.Equal(2, sent.Count);
        Assert.Same(first, sent[0]);
        Assert.Same(second, sent[1]);
    }

    [Fact]
    public async Task ARepeatedEntity_IsOneKey_AnsweredWithTheMocksFailure()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result = AsyncAlterClientQuotasResult.OfClient(
            admin.AlterClientQuotas(new[] { Alteration("dup", 1d), Alteration("dup", 2d) }, options: null));

        Assert.Equal(Entity("dup"), Assert.Single(result.Values).Key);
        await AssertNotImplemented(result.Values[Entity("dup")]);
    }

    [Fact]
    public async Task TwoEmptyEntities_AreOneKey()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ClientQuotaAlteration.Op[] noOps = Array.Empty<ClientQuotaAlteration.Op>();
        AsyncAlterClientQuotasResult result = AsyncAlterClientQuotasResult.OfClient(admin.AlterClientQuotas(
            new[] { new ClientQuotaAlteration(EmptyEntity(), noOps), new ClientQuotaAlteration(EmptyEntity(), noOps) },
            options: null));

        Assert.Equal(EmptyEntity(), Assert.Single(result.Values).Key);
        await AssertNotImplemented(result.Values[EmptyEntity()]);
    }

    [Fact]
    public async Task DistinctEntities_AreOneKeyEach_TheDefaultEntityIncluded()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterClientQuotasResult result = AsyncAlterClientQuotasResult.OfClient(admin.AlterClientQuotas(
            new[] { Alteration("a", 1d), Alteration(null, 2d), Alteration("a", 3d), Alteration(string.Empty, 4d) },
            options: null));

        Assert.Equal(3, result.Values.Count);
        Assert.Contains(Entity("a"), result.Values.Keys);
        Assert.Contains(Entity(null), result.Values.Keys);
        Assert.Contains(Entity(string.Empty), result.Values.Keys);
        await AssertNotImplemented(result.Values[Entity(null)]);
        await AssertNotImplemented(result.All());
    }

    private static async Task AssertNotImplemented(AsyncKafkaFuture future)
    {
        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => future.Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    private static ClientQuotaEntity Entity(string? user) =>
        new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [ClientQuotaEntity.User] = user,
            });

    private static ClientQuotaEntity EmptyEntity() =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal));

    private static ClientQuotaAlteration Alteration(string? user, double value) =>
        new ClientQuotaAlteration(
            Entity(user), new[] { new ClientQuotaAlteration.Op("producer_byte_rate", value) });
}
