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

using Confluent.Kafka.Common.Quota;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks the client-quota value types against their Java classes, above all their three-way distinctions: a filter
/// component matches an exact name, the default entity or any specified name; an entity's value is a name, the default
/// entity or absent; and an op sets a quota or clears it, <c>0.0</c> being a legal value.
/// </summary>
public class PublicClientQuotaTypesTests
{
    [Fact]
    public void ClientQuotaMatchType_HasTheAbiWireValues()
    {
        Assert.Equal(0, (int)ClientQuotaMatchType.Exact);
        Assert.Equal(1, (int)ClientQuotaMatchType.Default);
        Assert.Equal(2, (int)ClientQuotaMatchType.Specified);
    }

    [Fact]
    public void FilterComponent_ThreeFactories_ProduceThreeMatchTypes()
    {
        ClientQuotaFilterComponent exact =
            ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice");
        ClientQuotaFilterComponent dflt =
            ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User);
        ClientQuotaFilterComponent specified =
            ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User);

        Assert.Equal(ClientQuotaMatchType.Exact, exact.MatchType);
        Assert.Equal("alice", exact.MatchName);

        Assert.Equal(ClientQuotaMatchType.Default, dflt.MatchType);
        Assert.Null(dflt.MatchName);

        Assert.Equal(ClientQuotaMatchType.Specified, specified.MatchType);
        Assert.Null(specified.MatchName);

        Assert.Equal(ClientQuotaEntity.User, exact.EntityType);
        Assert.Equal(ClientQuotaEntity.User, dflt.EntityType);
        Assert.Equal(ClientQuotaEntity.User, specified.EntityType);
    }

    [Fact]
    public void FilterComponent_DefaultAndSpecified_AreDistinct()
    {
        ClientQuotaFilterComponent dflt =
            ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User);
        ClientQuotaFilterComponent specified =
            ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User);

        Assert.NotEqual(dflt, specified);
        Assert.NotEqual(dflt.GetHashCode(), specified.GetHashCode());

        // A dictionary is where a hash that merges two states fails silently.
        var byComponent = new Dictionary<ClientQuotaFilterComponent, string>
        {
            [dflt] = "default-user",
            [specified] = "any-named-user",
        };

        Assert.Equal(2, byComponent.Count);
        Assert.Equal("default-user", byComponent[ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User)]);
        Assert.Equal("any-named-user", byComponent[ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User)]);
    }

    [Fact]
    public void FilterComponent_ValueEquality()
    {
        ClientQuotaFilterComponent a = ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.Ip, "10.0.0.1");
        ClientQuotaFilterComponent b = ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.Ip, "10.0.0.1");

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());

        Assert.NotEqual(a, ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.Ip, "10.0.0.2"));
        Assert.NotEqual(a, ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "10.0.0.1"));
        Assert.False(a.Equals("not a component"));
    }

    [Fact]
    public void FilterComponent_RejectsNullArguments()
    {
        Assert.Equal(
            "entityName",
            Assert.Throws<ArgumentNullException>(
                () => ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, null!)).ParamName);

        Assert.Equal(
            "entityType",
            Assert.Throws<ArgumentNullException>(
                () => ClientQuotaFilterComponent.OfEntity(null!, "alice")).ParamName);

        Assert.Equal(
            "entityType",
            Assert.Throws<ArgumentNullException>(
                () => ClientQuotaFilterComponent.OfDefaultEntity(null!)).ParamName);

        Assert.Equal(
            "entityType",
            Assert.Throws<ArgumentNullException>(
                () => ClientQuotaFilterComponent.OfEntityType(null!)).ParamName);
    }

    [Fact]
    public void FilterComponent_HasNoPublicConstructor()
    {
        Assert.Empty(typeof(ClientQuotaFilterComponent).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
    }

    [Fact]
    public void FilterComponent_ToString_NamesTheMatchType()
    {
        Assert.Equal(
            "ClientQuotaFilterComponent(entityType=user, matchType=Exact, matchName=alice)",
            ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice").ToString());

        Assert.Equal(
            "ClientQuotaFilterComponent(entityType=user, matchType=Default, matchName=null)",
            ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User).ToString());

        Assert.Equal(
            "ClientQuotaFilterComponent(entityType=user, matchType=Specified, matchName=null)",
            ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User).ToString());
    }

    [Fact]
    public void Entity_EntityTypeConstants()
    {
        Assert.Equal("user", ClientQuotaEntity.User);
        Assert.Equal("client-id", ClientQuotaEntity.ClientId);
        Assert.Equal("ip", ClientQuotaEntity.Ip);

        Assert.True(ClientQuotaEntity.IsValidEntityType(ClientQuotaEntity.User));
        Assert.True(ClientQuotaEntity.IsValidEntityType(ClientQuotaEntity.ClientId));
        Assert.True(ClientQuotaEntity.IsValidEntityType(ClientQuotaEntity.Ip));

        Assert.False(ClientQuotaEntity.IsValidEntityType("User"));
        Assert.False(ClientQuotaEntity.IsValidEntityType("client_id"));
        Assert.False(ClientQuotaEntity.IsValidEntityType(""));
        Assert.False(ClientQuotaEntity.IsValidEntityType(null));
    }

    [Fact]
    public void Entity_NullValueIsNeitherEmptyNorAName()
    {
        var defaultUser = new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.User] = null });
        var emptyNamedUser = new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.User] = string.Empty });
        var namedUser = new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" });

        Assert.NotEqual(defaultUser, emptyNamedUser);
        Assert.NotEqual(defaultUser, namedUser);
        Assert.NotEqual(emptyNamedUser, namedUser);

        // Three distinct dictionary keys, as these arrive on the quota RPC results.
        var quotas = new Dictionary<ClientQuotaEntity, double>
        {
            [defaultUser] = 1.0,
            [emptyNamedUser] = 2.0,
            [namedUser] = 3.0,
        };

        Assert.Equal(3, quotas.Count);
        Assert.Equal(
            1.0,
            quotas[new ClientQuotaEntity(new Dictionary<string, string?> { [ClientQuotaEntity.User] = null })]);
        Assert.Equal(
            2.0,
            quotas[new ClientQuotaEntity(
                new Dictionary<string, string?> { [ClientQuotaEntity.User] = string.Empty })]);
        Assert.Equal(
            3.0,
            quotas[new ClientQuotaEntity(new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" })]);
    }

    [Fact]
    public void Entity_NullValueIsNotAnAbsentEntry()
    {
        var defaultUser = new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.User] = null });
        var empty = new ClientQuotaEntity(new Dictionary<string, string?>());

        Assert.NotEqual(defaultUser, empty);
        Assert.Single(defaultUser.Entries);
        Assert.Empty(empty.Entries);
    }

    [Fact]
    public void Entity_ValueEqualityIsOrderIndependent()
    {
        var a = new ClientQuotaEntity(new Dictionary<string, string?>
        {
            [ClientQuotaEntity.User] = "alice",
            [ClientQuotaEntity.ClientId] = null,
        });
        var b = new ClientQuotaEntity(new Dictionary<string, string?>
        {
            [ClientQuotaEntity.ClientId] = null,
            [ClientQuotaEntity.User] = "alice",
        });

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());

        var different = new ClientQuotaEntity(new Dictionary<string, string?>
        {
            [ClientQuotaEntity.User] = "alice",
            [ClientQuotaEntity.ClientId] = "app",
        });

        Assert.NotEqual(a, different);
        Assert.False(a.Equals("not an entity"));
    }

    [Fact]
    public void Entity_CopiesItsEntries()
    {
        var source = new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" };
        var entity = new ClientQuotaEntity(source);

        source[ClientQuotaEntity.User] = "bob";
        source[ClientQuotaEntity.Ip] = "10.0.0.1";

        Assert.Single(entity.Entries);
        Assert.Equal("alice", entity.Entries[ClientQuotaEntity.User]);
    }

    [Fact]
    public void Entity_RejectsNullEntries()
    {
        Assert.Equal(
            "entries",
            Assert.Throws<ArgumentNullException>(() => new ClientQuotaEntity(null!)).ParamName);
    }

    [Fact]
    public void Entity_ToString()
    {
        Assert.Equal(
            "ClientQuotaEntity(entries={user=null})",
            new ClientQuotaEntity(new Dictionary<string, string?> { [ClientQuotaEntity.User] = null })
                .ToString());

        Assert.Equal(
            "ClientQuotaEntity(entries={user=alice})",
            new ClientQuotaEntity(new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" })
                .ToString());
    }

    [Fact]
    public void Filter_ThreeFactories()
    {
        var components = new[] { ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User) };

        ClientQuotaFilter contains = ClientQuotaFilter.Contains(components);
        ClientQuotaFilter containsOnly = ClientQuotaFilter.ContainsOnly(components);
        ClientQuotaFilter all = ClientQuotaFilter.All();

        Assert.False(contains.Strict);
        Assert.Single(contains.Components);

        Assert.True(containsOnly.Strict);
        Assert.Single(containsOnly.Components);

        Assert.False(all.Strict);
        Assert.Empty(all.Components);

        Assert.NotEqual(contains, containsOnly);
    }

    [Fact]
    public void Filter_ValueEquality()
    {
        ClientQuotaFilter a = ClientQuotaFilter.Contains(new[]
        {
            ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice"),
            ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.ClientId),
        });
        ClientQuotaFilter b = ClientQuotaFilter.Contains(new[]
        {
            ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice"),
            ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.ClientId),
        });

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.Equal(ClientQuotaFilter.All(), ClientQuotaFilter.All());

        ClientQuotaFilter specified = ClientQuotaFilter.Contains(new[]
        {
            ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice"),
            ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.ClientId),
        });

        Assert.NotEqual(a, specified);
        Assert.False(a.Equals("not a filter"));
    }

    [Fact]
    public void Filter_CopiesItsComponents()
    {
        var source = new[] { ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User) };
        ClientQuotaFilter filter = ClientQuotaFilter.Contains(source);

        source[0] = ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.Ip, "10.0.0.1");

        Assert.Equal(
            ClientQuotaFilter.Contains(new[] { ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User) }),
            filter);
    }

    [Fact]
    public void Filter_RejectsNullComponents()
    {
        Assert.Equal(
            "components",
            Assert.Throws<ArgumentNullException>(() => ClientQuotaFilter.Contains(null!)).ParamName);
        Assert.Equal(
            "components",
            Assert.Throws<ArgumentNullException>(() => ClientQuotaFilter.ContainsOnly(null!)).ParamName);

        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => ClientQuotaFilter.Contains(new ClientQuotaFilterComponent[] { null! }));

        Assert.StartsWith("components must not contain null", ex.Message, StringComparison.Ordinal);
        Assert.Equal("components", ex.ParamName);
    }

    [Fact]
    public void Filter_ToString()
    {
        Assert.Equal("ClientQuotaFilter(components=[], strict=false)", ClientQuotaFilter.All().ToString());

        Assert.Equal(
            "ClientQuotaFilter(components=[ClientQuotaFilterComponent(entityType=user, matchType=Default, "
                + "matchName=null)], strict=true)",
            ClientQuotaFilter.ContainsOnly(
                new[] { ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User) }).ToString());
    }

    [Fact]
    public void Op_NullValueIsNotZero()
    {
        var clear = new ClientQuotaAlteration.Op("producer_byte_rate", null);
        var zero = new ClientQuotaAlteration.Op("producer_byte_rate", 0.0);

        Assert.Null(clear.Value);
        Assert.Equal(0.0, zero.Value!.Value);
        Assert.NotEqual(clear, zero);

        Assert.Equal(clear, new ClientQuotaAlteration.Op("producer_byte_rate", null));
        Assert.Equal(zero, new ClientQuotaAlteration.Op("producer_byte_rate", 0.0));
    }

    [Fact]
    public void Op_ValueEquality()
    {
        var a = new ClientQuotaAlteration.Op("consumer_byte_rate", 1024.0);
        var b = new ClientQuotaAlteration.Op("consumer_byte_rate", 1024.0);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());

        Assert.NotEqual(a, new ClientQuotaAlteration.Op("consumer_byte_rate", 2048.0));
        Assert.NotEqual(a, new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0));
        Assert.False(a.Equals("not an op"));

        Assert.Equal("key", Assert.Throws<ArgumentNullException>(
            () => new ClientQuotaAlteration.Op(null!, 1.0)).ParamName);
    }

    [Fact]
    public void Alteration_CarriesEntityAndOps()
    {
        var entity = new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" });
        var ops = new[]
        {
            new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0),
            new ClientQuotaAlteration.Op("consumer_byte_rate", null),
        };

        var alteration = new ClientQuotaAlteration(entity, ops);

        Assert.Equal(entity, alteration.Entity);
        Assert.Equal(2, alteration.Ops.Count);
        Assert.Contains(new ClientQuotaAlteration.Op("consumer_byte_rate", null), alteration.Ops);

        ops[0] = new ClientQuotaAlteration.Op("producer_byte_rate", 9999.0);
        Assert.Contains(new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0), alteration.Ops);
    }

    [Fact]
    public void Alteration_RejectsNullArguments()
    {
        var entity = new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" });

        Assert.Equal(
            "entity",
            Assert.Throws<ArgumentNullException>(
                () => new ClientQuotaAlteration(null!, Array.Empty<ClientQuotaAlteration.Op>())).ParamName);

        Assert.Equal(
            "ops",
            Assert.Throws<ArgumentNullException>(() => new ClientQuotaAlteration(entity, null!)).ParamName);

        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new ClientQuotaAlteration(entity, new ClientQuotaAlteration.Op[] { null! }));

        Assert.StartsWith("ops must not contain null", ex.Message, StringComparison.Ordinal);
        Assert.Equal("ops", ex.ParamName);
    }

    [Fact]
    public void Alteration_UsesReferenceEqualityLikeJava()
    {
        var entity = new ClientQuotaEntity(
            new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" });
        var ops = new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0) };

        var a = new ClientQuotaAlteration(entity, ops);
        var b = new ClientQuotaAlteration(entity, ops);

        Assert.NotEqual(a, b);
        Assert.Equal(a, a);
    }

    [Fact]
    public void Alteration_ToString()
    {
        Assert.Equal(
            "ClientQuotaAlteration.Op(key=producer_byte_rate, value=null)",
            new ClientQuotaAlteration.Op("producer_byte_rate", null).ToString());

        Assert.Equal(
            "ClientQuotaAlteration.Op(key=producer_byte_rate, value=0)",
            new ClientQuotaAlteration.Op("producer_byte_rate", 0.0).ToString());

        var alteration = new ClientQuotaAlteration(
            new ClientQuotaEntity(new Dictionary<string, string?> { [ClientQuotaEntity.User] = "alice" }),
            new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0) });

        Assert.Equal(
            "ClientQuotaAlteration(entity=ClientQuotaEntity(entries={user=alice}), "
                + "ops=[ClientQuotaAlteration.Op(key=producer_byte_rate, value=1024)])",
            alteration.ToString());
    }
}
