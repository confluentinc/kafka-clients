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

using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Java.Lang;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks the ACL value types against their Java classes: the enums carry the wire codes, the concrete types reject
/// null strings and the <c>ANY</c> / <c>MATCH</c> sentinels while the filters accept both, a filter's
/// <see langword="null"/> (match any) differs from <c>""</c> (the empty name), and equality holds through a dictionary
/// lookup.
/// </summary>
public sealed class PublicAclTypesTests
{
    [Theory]
    [InlineData(ResourceType.UNKNOWN, 0)]
    [InlineData(ResourceType.ANY, 1)]
    [InlineData(ResourceType.TOPIC, 2)]
    [InlineData(ResourceType.GROUP, 3)]
    [InlineData(ResourceType.CLUSTER, 4)]
    [InlineData(ResourceType.TRANSACTIONAL_ID, 5)]
    [InlineData(ResourceType.DELEGATION_TOKEN, 6)]
    [InlineData(ResourceType.USER, 7)]
    public void ResourceType_MemberValuesAreJavaWireCodes(ResourceType value, int code) =>
        Assert.Equal(code, (int)value);

    [Theory]
    [InlineData(PatternType.UNKNOWN, 0)]
    [InlineData(PatternType.ANY, 1)]
    [InlineData(PatternType.MATCH, 2)]
    [InlineData(PatternType.LITERAL, 3)]
    [InlineData(PatternType.PREFIXED, 4)]
    public void PatternType_MemberValuesAreJavaWireCodes(PatternType value, int code) =>
        Assert.Equal(code, (int)value);

    [Theory]
    [InlineData(AclPermissionType.UNKNOWN, 0)]
    [InlineData(AclPermissionType.ANY, 1)]
    [InlineData(AclPermissionType.DENY, 2)]
    [InlineData(AclPermissionType.ALLOW, 3)]
    public void AclPermissionType_MemberValuesAreJavaWireCodes(AclPermissionType value, int code) =>
        Assert.Equal(code, (int)value);

    [Fact]
    public void Enums_DeclareExactlyJavasMemberCounts()
    {
        Assert.Equal(8, Enum.GetValues(typeof(ResourceType)).Length);
        Assert.Equal(5, Enum.GetValues(typeof(PatternType)).Length);
        Assert.Equal(4, Enum.GetValues(typeof(AclPermissionType)).Length);
    }

    [Fact]
    public void ResourcePattern_ExposesItsComponents()
    {
        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL);

        Assert.Equal(ResourceType.TOPIC, pattern.ResourceType);
        Assert.Equal("orders", pattern.Name);
        Assert.Equal(PatternType.LITERAL, pattern.PatternType);
        Assert.False(pattern.IsUnknown);
    }

    [Fact]
    public void ResourcePattern_WildcardResourceIsStar() =>
        Assert.Equal("*", ResourcePattern.WILDCARD_RESOURCE);

    [Fact]
    public void ResourcePattern_NullName_Throws()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ResourcePattern(ResourceType.TOPIC, null!, PatternType.LITERAL));

        Assert.Equal("name", ex.ParamName);
    }

    [Fact]
    public void ResourcePattern_AnyResourceType_ThrowsWithJavasMessage()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new ResourcePattern(ResourceType.ANY, "orders", PatternType.LITERAL));

        Assert.Equal("resourceType must not be ANY", ex.Message);
    }

    [Theory]
    [InlineData(PatternType.ANY, "patternType must not be ANY")]
    [InlineData(PatternType.MATCH, "patternType must not be MATCH")]
    public void ResourcePattern_FilterOnlyPatternType_ThrowsWithJavasMessage(
        PatternType patternType,
        string expected)
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new ResourcePattern(ResourceType.TOPIC, "orders", patternType));

        Assert.Equal(expected, ex.Message);
    }

    [Fact]
    public void ResourcePattern_EmptyName_IsAccepted() =>
        Assert.Equal(string.Empty, new ResourcePattern(ResourceType.TOPIC, string.Empty, PatternType.LITERAL).Name);

    [Fact]
    public void ResourcePattern_IsUnknown_TracksEitherComponent()
    {
        Assert.True(new ResourcePattern(ResourceType.UNKNOWN, "orders", PatternType.LITERAL).IsUnknown);
        Assert.True(new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.UNKNOWN).IsUnknown);
        Assert.False(new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.PREFIXED).IsUnknown);
    }

    [Fact]
    public void ResourcePattern_ToFilter_CarriesEveryComponent()
    {
        ResourcePatternFilter filter =
            new ResourcePattern(ResourceType.GROUP, "g1", PatternType.PREFIXED).ToFilter();

        Assert.Equal(new ResourcePatternFilter(ResourceType.GROUP, "g1", PatternType.PREFIXED), filter);
    }

    [Fact]
    public void ResourcePattern_ToString_MatchesJava() =>
        Assert.Equal(
            "ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL)",
            new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL).ToString());

    [Fact]
    public void ResourcePatternFilter_AcceptsAnyAndMatchAndNullName()
    {
        ResourcePatternFilter filter = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.MATCH);

        Assert.Equal(ResourceType.ANY, filter.ResourceType);
        Assert.Null(filter.Name);
        Assert.Equal(PatternType.MATCH, filter.PatternType);
    }

    [Fact]
    public void ResourcePatternFilter_Any_IsTheMatchEverythingFilter()
    {
        Assert.Equal(ResourceType.ANY, ResourcePatternFilter.ANY.ResourceType);
        Assert.Null(ResourcePatternFilter.ANY.Name);
        Assert.Equal(PatternType.ANY, ResourcePatternFilter.ANY.PatternType);
    }

    [Fact]
    public void ResourcePatternFilter_NullName_IsNotEmptyName()
    {
        ResourcePatternFilter wildcard = new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL);
        ResourcePatternFilter empty =
            new ResourcePatternFilter(ResourceType.TOPIC, string.Empty, PatternType.LITERAL);

        Assert.NotEqual(wildcard, empty);
        Assert.Null(wildcard.Name);
        Assert.Equal(string.Empty, empty.Name);

        // The distinction has to survive a dictionary, which is where these types are used.
        Dictionary<ResourcePatternFilter, string> byFilter = new Dictionary<ResourcePatternFilter, string>
        {
            [wildcard] = "wildcard",
            [empty] = "empty",
        };

        Assert.Equal(2, byFilter.Count);
        Assert.Equal("wildcard", byFilter[new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL)]);
        Assert.Equal(
            "empty",
            byFilter[new ResourcePatternFilter(ResourceType.TOPIC, string.Empty, PatternType.LITERAL)]);
    }

    [Fact]
    public void ResourcePatternFilter_ValueEquality()
    {
        ResourcePatternFilter a = new ResourcePatternFilter(ResourceType.TOPIC, "orders", PatternType.PREFIXED);
        ResourcePatternFilter b = new ResourcePatternFilter(ResourceType.TOPIC, "orders", PatternType.PREFIXED);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.GROUP, "orders", PatternType.PREFIXED));
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.TOPIC, "Orders", PatternType.PREFIXED));
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.TOPIC, "orders", PatternType.LITERAL));
        Assert.False(a.Equals("not a filter"));
    }

    [Fact]
    public void ResourcePatternFilter_ToString_MatchesJava()
    {
        Assert.Equal(
            "ResourcePattern(resourceType=ANY, name=<any>, patternType=ANY)",
            ResourcePatternFilter.ANY.ToString());
        Assert.Equal(
            "ResourcePattern(resourceType=TOPIC, name=, patternType=LITERAL)",
            new ResourcePatternFilter(ResourceType.TOPIC, string.Empty, PatternType.LITERAL).ToString());
    }

    [Fact]
    public void AccessControlEntry_ExposesItsComponents()
    {
        AccessControlEntry entry =
            new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW);

        Assert.Equal("User:alice", entry.Principal);
        Assert.Equal("*", entry.Host);
        Assert.Equal(AclOperation.READ, entry.Operation);
        Assert.Equal(AclPermissionType.ALLOW, entry.PermissionType);
        Assert.False(entry.IsUnknown);
    }

    [Fact]
    public void AccessControlEntry_NullStrings_Throw()
    {
        Assert.Equal(
            "principal",
            Assert.Throws<ArgumentNullException>(
                () => new AccessControlEntry(null!, "*", AclOperation.READ, AclPermissionType.ALLOW)).ParamName);

        Assert.Equal(
            "host",
            Assert.Throws<ArgumentNullException>(
                () => new AccessControlEntry("User:alice", null!, AclOperation.READ, AclPermissionType.ALLOW))
                .ParamName);
    }

    [Fact]
    public void AccessControlEntry_AnyOperation_ThrowsWithJavasMessage()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.ANY, AclPermissionType.ALLOW));

        Assert.Equal("operation must not be ANY", ex.Message);
    }

    [Fact]
    public void AccessControlEntry_AnyPermissionType_ThrowsWithJavasMessage()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ANY));

        Assert.Equal("permissionType must not be ANY", ex.Message);
    }

    [Fact]
    public void AccessControlEntry_EmptyStrings_AreAccepted()
    {
        AccessControlEntry entry = new AccessControlEntry(
            string.Empty, string.Empty, AclOperation.DESCRIBE, AclPermissionType.DENY);

        Assert.Equal(string.Empty, entry.Principal);
        Assert.Equal(string.Empty, entry.Host);
    }

    [Fact]
    public void AccessControlEntry_IsUnknown_TracksEitherEnum()
    {
        Assert.True(new AccessControlEntry("User:a", "*", AclOperation.UNKNOWN, AclPermissionType.ALLOW).IsUnknown);
        Assert.True(new AccessControlEntry("User:a", "*", AclOperation.READ, AclPermissionType.UNKNOWN).IsUnknown);
    }

    [Fact]
    public void AccessControlEntry_ToFilter_CarriesEveryComponent()
    {
        AccessControlEntryFilter filter =
            new AccessControlEntry("User:alice", "10.0.0.1", AclOperation.WRITE, AclPermissionType.DENY).ToFilter();

        Assert.Equal(
            new AccessControlEntryFilter("User:alice", "10.0.0.1", AclOperation.WRITE, AclPermissionType.DENY),
            filter);
    }

    [Fact]
    public void AccessControlEntry_ToString_MatchesJava() =>
        Assert.Equal(
            "(principal=User:alice, host=*, operation=READ, permissionType=ALLOW)",
            new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW).ToString());

    [Fact]
    public void AccessControlEntryFilter_AcceptsNullsAndAny()
    {
        AccessControlEntryFilter filter =
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);

        Assert.Null(filter.Principal);
        Assert.Null(filter.Host);
        Assert.Equal(AclOperation.ANY, filter.Operation);
        Assert.Equal(AclPermissionType.ANY, filter.PermissionType);
    }

    [Fact]
    public void AccessControlEntryFilter_Any_IsTheMatchEverythingFilter() =>
        Assert.Equal(
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY),
            AccessControlEntryFilter.ANY);

    [Fact]
    public void AccessControlEntryFilter_NullStrings_AreNotEmptyStrings()
    {
        AccessControlEntryFilter nullPrincipal =
            new AccessControlEntryFilter(null, "*", AclOperation.READ, AclPermissionType.ALLOW);
        AccessControlEntryFilter emptyPrincipal =
            new AccessControlEntryFilter(string.Empty, "*", AclOperation.READ, AclPermissionType.ALLOW);
        AccessControlEntryFilter nullHost =
            new AccessControlEntryFilter("User:a", null, AclOperation.READ, AclPermissionType.ALLOW);
        AccessControlEntryFilter emptyHost =
            new AccessControlEntryFilter("User:a", string.Empty, AclOperation.READ, AclPermissionType.ALLOW);

        Assert.NotEqual(nullPrincipal, emptyPrincipal);
        Assert.NotEqual(nullHost, emptyHost);

        Dictionary<AccessControlEntryFilter, string> byFilter = new Dictionary<AccessControlEntryFilter, string>
        {
            [nullPrincipal] = "null-principal",
            [emptyPrincipal] = "empty-principal",
            [nullHost] = "null-host",
            [emptyHost] = "empty-host",
        };

        Assert.Equal(4, byFilter.Count);
        Assert.Equal(
            "empty-principal",
            byFilter[new AccessControlEntryFilter(string.Empty, "*", AclOperation.READ, AclPermissionType.ALLOW)]);
    }

    [Fact]
    public void AccessControlEntryFilter_ValueEquality()
    {
        AccessControlEntryFilter a =
            new AccessControlEntryFilter("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW);
        AccessControlEntryFilter b =
            new AccessControlEntryFilter("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, new AccessControlEntryFilter("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW));
        Assert.NotEqual(
            a,
            new AccessControlEntryFilter("User:alice", "*", AclOperation.WRITE, AclPermissionType.ALLOW));
        Assert.NotEqual(
            a,
            new AccessControlEntryFilter("User:alice", "*", AclOperation.READ, AclPermissionType.DENY));
        Assert.False(a.Equals(42));
    }

    [Fact]
    public void AccessControlEntryFilter_ToString_RendersNullAsAny() =>
        Assert.Equal(
            "(principal=<any>, host=<any>, operation=ANY, permissionType=ANY)",
            AccessControlEntryFilter.ANY.ToString());

    [Fact]
    public void AclBinding_IsNested_NotFlattened()
    {
        AclBinding binding = NewBinding();

        Assert.Equal("orders", binding.Pattern.Name);
        Assert.Equal(ResourceType.TOPIC, binding.Pattern.ResourceType);
        Assert.Equal(PatternType.LITERAL, binding.Pattern.PatternType);
        Assert.Equal("User:alice", binding.Entry.Principal);
        Assert.Equal("*", binding.Entry.Host);
        Assert.Equal(AclOperation.READ, binding.Entry.Operation);
        Assert.Equal(AclPermissionType.ALLOW, binding.Entry.PermissionType);
        Assert.False(binding.IsUnknown);
    }

    [Fact]
    public void AclBinding_NullHalves_Throw()
    {
        AccessControlEntry entry =
            new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW);
        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL);

        Assert.Equal("pattern", Assert.Throws<ArgumentNullException>(() => new AclBinding(null!, entry)).ParamName);
        Assert.Equal("entry", Assert.Throws<ArgumentNullException>(() => new AclBinding(pattern, null!)).ParamName);
    }

    [Fact]
    public void AclBinding_IsUnknown_PropagatesFromEitherHalf()
    {
        Assert.True(new AclBinding(
            new ResourcePattern(ResourceType.UNKNOWN, "orders", PatternType.LITERAL),
            new AccessControlEntry("User:a", "*", AclOperation.READ, AclPermissionType.ALLOW)).IsUnknown);

        Assert.True(new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
            new AccessControlEntry("User:a", "*", AclOperation.READ, AclPermissionType.UNKNOWN)).IsUnknown);
    }

    [Fact]
    public void AclBinding_ToFilter_CarriesBothHalves()
    {
        AclBindingFilter filter = NewBinding().ToFilter();

        Assert.Equal(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntryFilter("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW)),
            filter);
    }

    [Fact]
    public void AclBinding_ToString_MatchesJava() =>
        Assert.Equal(
            "(pattern=ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL), "
                + "entry=(principal=User:alice, host=*, operation=READ, permissionType=ALLOW))",
            NewBinding().ToString());

    [Fact]
    public void AclBinding_IsUsableAsADictionaryKey()
    {
        Dictionary<AclBinding, string> byBinding = new Dictionary<AclBinding, string>
        {
            [NewBinding()] = "created",
        };

        Assert.True(byBinding.ContainsKey(NewBinding()));
        Assert.Equal("created", byBinding[NewBinding()]);
        Assert.Equal(NewBinding().GetHashCode(), NewBinding().GetHashCode());

        AclBinding other = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "payments", PatternType.LITERAL),
            new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));

        Assert.False(byBinding.ContainsKey(other));
        Assert.NotEqual(NewBinding(), other);
        Assert.False(NewBinding().Equals("not a binding"));
    }

    [Fact]
    public void AclBindingFilter_Any_IsBothHalvesAny()
    {
        Assert.Equal(ResourcePatternFilter.ANY, AclBindingFilter.ANY.PatternFilter);
        Assert.Equal(AccessControlEntryFilter.ANY, AclBindingFilter.ANY.EntryFilter);
        Assert.Equal(
            new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY),
            AclBindingFilter.ANY);
    }

    [Fact]
    public void AclBindingFilter_NullHalves_Throw()
    {
        Assert.Equal(
            "patternFilter",
            Assert.Throws<ArgumentNullException>(
                () => new AclBindingFilter(null!, AccessControlEntryFilter.ANY)).ParamName);

        Assert.Equal(
            "entryFilter",
            Assert.Throws<ArgumentNullException>(
                () => new AclBindingFilter(ResourcePatternFilter.ANY, null!)).ParamName);
    }

    [Fact]
    public void AclBindingFilter_IsUnknown_PropagatesFromEitherHalf()
    {
        Assert.False(AclBindingFilter.ANY.IsUnknown);

        Assert.True(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.ANY),
            AccessControlEntryFilter.ANY).IsUnknown);

        Assert.True(new AclBindingFilter(
            ResourcePatternFilter.ANY,
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.UNKNOWN)).IsUnknown);
    }

    [Fact]
    public void AclBindingFilter_IsUsableAsADictionaryKey_AndKeepsNullApartFromEmpty()
    {
        AclBindingFilter wildcardName = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL),
            AccessControlEntryFilter.ANY);
        AclBindingFilter emptyName = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, string.Empty, PatternType.LITERAL),
            AccessControlEntryFilter.ANY);

        Assert.NotEqual(wildcardName, emptyName);

        Dictionary<AclBindingFilter, string> byFilter = new Dictionary<AclBindingFilter, string>
        {
            [wildcardName] = "wildcard",
            [emptyName] = "empty",
        };

        Assert.Equal(2, byFilter.Count);
        Assert.Equal(
            "wildcard",
            byFilter[new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, null, PatternType.LITERAL),
                AccessControlEntryFilter.ANY)]);
        Assert.Equal(
            "empty",
            byFilter[new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, string.Empty, PatternType.LITERAL),
                AccessControlEntryFilter.ANY)]);
        Assert.False(wildcardName.Equals("not a filter"));
    }

    [Fact]
    public void AclBindingFilter_ToString_MatchesJava() =>
        Assert.Equal(
            "(patternFilter=ResourcePattern(resourceType=ANY, name=<any>, patternType=ANY), "
                + "entryFilter=(principal=<any>, host=<any>, operation=ANY, permissionType=ANY))",
            AclBindingFilter.ANY.ToString());

    [Fact]
    public void AclBinding_And_Filter_RoundTripThroughTheirComponents()
    {
        AclBinding binding = NewBinding();
        AclBindingFilter filter = binding.ToFilter();

        Assert.Equal(binding.Pattern.ResourceType, filter.PatternFilter.ResourceType);
        Assert.Equal(binding.Pattern.Name, filter.PatternFilter.Name);
        Assert.Equal(binding.Pattern.PatternType, filter.PatternFilter.PatternType);
        Assert.Equal(binding.Entry.Principal, filter.EntryFilter.Principal);
        Assert.Equal(binding.Entry.Host, filter.EntryFilter.Host);
        Assert.Equal(binding.Entry.Operation, filter.EntryFilter.Operation);
        Assert.Equal(binding.Entry.PermissionType, filter.EntryFilter.PermissionType);
    }

    private static AclBinding NewBinding() =>
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
            new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
}
