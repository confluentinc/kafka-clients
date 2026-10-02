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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the client-quota value-type family (<see cref="ClientQuotaMatchType"/>,
/// <see cref="ClientQuotaEntity"/>, <see cref="ClientQuotaFilterComponent"/>,
/// <see cref="ClientQuotaFilter"/>, <see cref="ClientQuotaAlteration"/>) against the Java classes
/// it restores. The family's defining property is that three of its distinctions are
/// <b>ternary</b>, not binary: a filter component matches an exact name / the built-in default /
/// any specified name; an entity's entry value is a name / the default entity / absent; and an
/// op's value sets a quota / clears it, where <c>0.0</c> is a legal value.
/// </summary>
public class PublicClientQuotaTypesTests
{
    // ---- ClientQuotaMatchType: the ABI wire constants ----

    /// <summary>The enum values are the ABI's <c>match_types</c> constants, not arbitrary.</summary>
    [Fact]
    public void ClientQuotaMatchType_HasTheAbiWireValues()
    {
        Assert.Equal(0, (int)ClientQuotaMatchType.Exact);
        Assert.Equal(1, (int)ClientQuotaMatchType.Default);
        Assert.Equal(2, (int)ClientQuotaMatchType.Specified);
    }

    // ---- ClientQuotaFilterComponent: the three-state match ----

    /// <summary>
    /// Java's three factories (<c>:51</c> / <c>:61</c> / <c>:71</c>) produce three distinct match
    /// types, with a name only on the exact one.
    /// </summary>
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

    /// <summary>
    /// The one defect a <c>string?</c> model would ship: "the default user's quota" and "every
    /// named user's quota" must not compare — or hash — alike, although neither carries a name
    /// (PLAN §2.2 / D37).
    /// </summary>
    [Fact]
    public void FilterComponent_DefaultAndSpecified_AreDistinct()
    {
        ClientQuotaFilterComponent dflt =
            ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User);
        ClientQuotaFilterComponent specified =
            ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User);

        Assert.NotEqual(dflt, specified);
        Assert.NotEqual(dflt.GetHashCode(), specified.GetHashCode());

        // And a dictionary — the shape that fails silently when a hash collapses two states.
        var byComponent = new Dictionary<ClientQuotaFilterComponent, string>
        {
            [dflt] = "default-user",
            [specified] = "any-named-user",
        };

        Assert.Equal(2, byComponent.Count);
        Assert.Equal("default-user", byComponent[ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.User)]);
        Assert.Equal("any-named-user", byComponent[ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User)]);
    }

    /// <summary>Equal components compare and hash alike — Java's <c>equals</c> (<c>:93</c>).</summary>
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

    /// <summary>
    /// <c>OfEntity</c> keeps Java's <c>Objects.requireNonNull(entityName)</c> (<c>:51</c>); every
    /// factory keeps the private ctor's <c>requireNonNull(entityType)</c> (<c>:39</c>).
    /// </summary>
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

    /// <summary>
    /// No public constructor (Java's is private, <c>:39</c>), so the illegal fourth state — an
    /// <see cref="ClientQuotaMatchType.Exact"/> match with no name — is unrepresentable.
    /// </summary>
    [Fact]
    public void FilterComponent_HasNoPublicConstructor()
    {
        Assert.Empty(typeof(ClientQuotaFilterComponent).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
    }

    /// <summary>The rendering names the match type, so the three states are distinguishable in logs.</summary>
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

    // ---- ClientQuotaEntity ----

    /// <summary>The three entity-type constants and the validity predicate — Java <c>:33-37</c>.</summary>
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

    /// <summary>
    /// The second ternary: a <c>null</c> entry value names the built-in default entity, <c>""</c>
    /// is a real (empty) name, and a name is a name — three distinct entities, which a
    /// <c>IReadOnlyDictionary&lt;string, string&gt;</c> could not express.
    /// </summary>
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

        // Three distinct dictionary keys — how these arrive on the quota RPC results (PLAN D39).
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

    /// <summary>
    /// A null entry value is also not the same as <em>omitting</em> the type — one names the
    /// default user, the other constrains nothing about users.
    /// </summary>
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

    /// <summary>
    /// Value equality is entry-wise and order-independent — Java delegates to <c>Map.equals</c>
    /// (<c>:61</c>).
    /// </summary>
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

    /// <summary>
    /// The entries are copied, so mutating the source map cannot silently change a live key's
    /// hash.
    /// </summary>
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

    /// <summary>A null map is a precondition failure, not a later null-reference.</summary>
    [Fact]
    public void Entity_RejectsNullEntries()
    {
        Assert.Equal(
            "entries",
            Assert.Throws<ArgumentNullException>(() => new ClientQuotaEntity(null!)).ParamName);
    }

    /// <summary>The rendering shows a default-entity entry as <c>null</c> — Java <c>:74</c>.</summary>
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

    // ---- ClientQuotaFilter ----

    /// <summary>
    /// <c>Contains</c> (<c>:49</c>) is non-strict, <c>ContainsOnly</c> (<c>:59</c>) is strict, and
    /// <c>All</c> (<c>:66</c>) is empty and non-strict.
    /// </summary>
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

        // Strictness is part of the value: same components, different filter.
        Assert.NotEqual(contains, containsOnly);
    }

    /// <summary>Equal filters compare and hash alike — Java's <c>equals</c> (<c>:85</c>).</summary>
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

        // The component ternary carries through the filter: swapping Default for Specified is a
        // different filter, not a cosmetic difference.
        ClientQuotaFilter specified = ClientQuotaFilter.Contains(new[]
        {
            ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice"),
            ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.ClientId),
        });

        Assert.NotEqual(a, specified);
        Assert.False(a.Equals("not a filter"));
    }

    /// <summary>The components are copied, so mutating the source array cannot alter the filter.</summary>
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

    /// <summary>Null components are rejected before they can reach the boundary (ffi §A5).</summary>
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

    /// <summary>The rendering carries both halves — Java <c>:98</c>.</summary>
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

    // ---- ClientQuotaAlteration (+ Op) ----

    /// <summary>
    /// The third ternary: <c>Op(key, null)</c> <em>clears</em> the quota while
    /// <c>Op(key, 0.0)</c> sets it to zero — <c>0.0</c> being a legal quota value, no sentinel
    /// could carry the distinction.
    /// </summary>
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

    /// <summary>Value equality over key and value — Java's <c>Op.equals</c> (<c>:58</c>).</summary>
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

    /// <summary>The alteration pairs an entity with its ops — Java <c>:83</c>.</summary>
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

        // Copied, so a later mutation of the caller's array does not reach the alteration.
        ops[0] = new ClientQuotaAlteration.Op("producer_byte_rate", 9999.0);
        Assert.Contains(new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0), alteration.Ops);
    }

    /// <summary>Preconditions are checked before anything can reach the boundary (ffi §A5).</summary>
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

    /// <summary>
    /// Java declares no <c>equals</c> / <c>hashCode</c> on <c>ClientQuotaAlteration</c> (only on
    /// <c>Op</c>), so reference equality is the faithful surface — pinned so a later phase does
    /// not add value equality by reflex.
    /// </summary>
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

    /// <summary>The renderings carry the clear-vs-zero distinction — Java <c>:71</c> / <c>:103</c>.</summary>
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
