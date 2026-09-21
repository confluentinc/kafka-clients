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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>alterClientQuotas</c> end to end against <see cref="MockAdminClient"/>, no broker
/// (M15/P6).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>There is no happy path here, and that is Java's behaviour rather than a gap.</b>
/// Java's <c>MockAdminClient.alterClientQuotas</c> throws
/// <c>UnsupportedOperationException("Not implement yet")</c> — Java's own typo, mirrored
/// (<c>MockAdminClient.java:1249-1251</c>) — and the Rust mock surfaces it as one exceptional
/// future <em>per entity</em> (<c>admin-client.md</c> §9). That still exercises the whole
/// receive path: the walk reads a real <c>kafka_admin_AlterClientQuotasResult_t</c>, rebuilds
/// each key from a real borrowed <c>kafka_common_ClientQuotaEntity_t</c>, and reads each
/// per-entity error through <see cref="KafkaException.FromBorrowedHandle"/>.
/// </para>
/// <para>
/// ⚠⚠ Every lookup below goes through <see cref="ClientQuotaEntity"/>'s value equality, and
/// several deliberately use a <em>freshly constructed</em> entity rather than the instance
/// that was submitted (PLAN D39, T-N6).
/// </para>
/// </remarks>
public sealed class PublicAdminAlterClientQuotasTests
{
    private const string NotImplemented = "Not implement yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly ClientQuotaAlteration.Op[] s_noOps =
        Array.Empty<ClientQuotaAlteration.Op>();

    /// <summary>
    /// ⚠ <b>T-N6.</b> An entity built fresh, equal by value to the one submitted, finds its
    /// own awaitable in <see cref="AlterClientQuotasResult.Values"/>.
    /// </summary>
    [Fact]
    public async Task Values_AreKeyedByValue_NotByReference()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("keyed-by-value", 1d) });

        ClientQuotaEntity lookup = Entity("keyed-by-value");
        Assert.NotSame(lookup, Assert.Single(result.Values).Key);
        Assert.True(result.Values.ContainsKey(lookup));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[lookup], s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// ⚠ <b>T-N5, read side.</b> The default entity (a null name), the entity named
    /// <c>""</c> and the entity named <c>"x"</c> are <b>three distinct</b> keys, both on the
    /// way out and on the way back.
    /// </summary>
    [Fact]
    public async Task TheEntityNameTernary_ProducesThreeDistinctKeys()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AlterClientQuotasResult result = admin.AlterClientQuotas(
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

        // null is not "", and neither is "x" — a collapsing model loses one of the three.
        Assert.NotEqual(Entity(null), Entity(string.Empty));

        // ⚠ The MESSAGE is what makes this discriminating end to end: if the marshaller
        // collapsed the null name into "", the three rows would become two duplicates and the
        // ABI would reject the whole request instead of reaching the mock.
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// Each entity gets <b>its own</b> awaitable, and the key the result hands back round
    /// trips through the ABI unchanged.
    /// </summary>
    [Fact]
    public async Task EachEntity_GetsItsOwnTask_AndItsKeyRoundTrips()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        ClientQuotaEntity alpha = new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                [ClientQuotaEntity.User] = "alice",
                [ClientQuotaEntity.ClientId] = "app-1",
            });
        ClientQuotaEntity beta = Entity("bob");

        AlterClientQuotasResult result = admin.AlterClientQuotas(
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
        Assert.NotSame(result.Values[alpha], result.Values[beta]);
        Assert.Contains(alpha, result.Values.Keys);
        Assert.Contains(beta, result.Values.Keys);

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[alpha], s_deadline));
        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[beta], s_deadline));
    }

    /// <summary>
    /// One entity can be awaited without awaiting the others — the per-key granularity Java
    /// publishes, and the discriminator against a single shared awaitable.
    /// </summary>
    [Fact]
    public async Task OneKey_IsAwaitableWithoutTheOthers()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        ClientQuotaEntity first = Entity("solo-first");
        AlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("solo-first", 1d), Alteration("solo-second", 2d) });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[first], s_deadline));
        Assert.Equal(NotImplemented, failure.Message);

        // Observe the other one too, so no task is left with an unobserved fault.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary><c>All()</c> faults when any entity failed — Java's <c>KafkaFuture.allOf</c>.</summary>
    [Fact]
    public async Task All_FaultsWhenAnyEntityFails()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AlterClientQuotasResult result = admin.AlterClientQuotas(
            new[] { Alteration("all-a", 1d), Alteration("all-b", 2d) });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// <c>All()</c> over an empty request completes — Java's <c>allOf</c> of nothing is a
    /// completed future.
    /// </summary>
    [Fact]
    public async Task All_OverAnEmptyRequest_Completes()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AlterClientQuotasResult result =
            admin.AlterClientQuotas(Array.Empty<ClientQuotaAlteration>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// ⚠ A non-ASCII entity name survives out and back — the UTF-8 round trip through both
    /// the ragged projector and the entity reader (ffi §A3/§B3).
    /// </summary>
    [Fact]
    public async Task NonAsciiName_RoundTripsThroughTheResultKey()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AlterClientQuotasResult result =
            admin.AlterClientQuotas(new[] { Alteration("用户-café", 1d) });

        ClientQuotaEntity key = Assert.Single(result.Values).Key;
        Assert.Equal("用户-café", key.Entries[ClientQuotaEntity.User]);
        Assert.Equal(Entity("用户-café"), key);

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// Preconditions are rejected before any native call (ffi §B5) — including the two ABI
    /// rejections C# surfaces itself (PLAN D38): an alteration with no entity types, and the
    /// same entity altered twice.
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

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

        ArgumentException empty = Assert.Throws<ArgumentException>(
            () => admin.AlterClientQuotas(
                new[]
                {
                    new ClientQuotaAlteration(
                        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal)),
                        s_noOps),
                }));
        Assert.Equal("entries", empty.ParamName);
        Assert.StartsWith(
            "The client quota alterations must not contain an alteration with no entity types.",
            empty.Message,
            StringComparison.Ordinal);

        // ⚠ Rejected, NOT collapsed: the ABI refuses a repeated entity (h:8300-8302), so
        // silently dropping one would lose an alteration the caller wrote.
        ArgumentException duplicate = Assert.Throws<ArgumentException>(
            () => admin.AlterClientQuotas(new[] { Alteration("dup", 1d), Alteration("dup", 2d) }));
        Assert.Equal("entries", duplicate.ParamName);
        Assert.StartsWith(
            "The client quota alterations must not alter the entity ClientQuotaEntity(entries={user=dup}) "
            + "more than once.",
            duplicate.Message,
            StringComparison.Ordinal);

        ArgumentOutOfRangeException negativeTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.AlterClientQuotas(
                new[] { Alteration("ok", 1d) }, new AlterClientQuotasOptions { TimeoutMs = -1 }));
        Assert.StartsWith(
            "AlterClientQuotasOptions.TimeoutMs must not be negative; leave it null to use the client default.",
            negativeTimeout.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A null <see cref="ClientQuotaAlteration"/> entity or op is rejected by the value type's
    /// own constructor, which is why the submit does not re-check them.
    /// </summary>
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

    /// <summary>
    /// Using the client after it is closed throws <see cref="ObjectDisposedException"/> rather
    /// than reaching a destroyed handle.
    /// </summary>
    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient(1);
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(
            () => admin.AlterClientQuotas(new[] { Alteration("gone", 1d) }));
    }

    /// <summary>
    /// <see cref="AlterClientQuotasOptions"/> ships Java's defaults: an unset timeout and
    /// <c>validateOnly = false</c> (<c>AlterClientQuotasOptions.java:27</c>).
    /// </summary>
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

    private static ClientQuotaAlteration Alteration(string user, double value) =>
        new ClientQuotaAlteration(
            Entity(user), new[] { new ClientQuotaAlteration.Op("producer_byte_rate", value) });
}
