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
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks the ACL value types against their Java classes: the enums carry the wire codes, the concrete types reject
/// null strings and the <c>Any</c> / <c>Match</c> sentinels while the filters accept both, a filter's
/// <see langword="null"/> (match any) differs from <c>""</c> (the empty name), and equality holds through a dictionary
/// lookup.
/// </summary>
public sealed class PublicAclTypesTests
{
    [Theory]
    [InlineData(ResourceType.Unknown, 0)]
    [InlineData(ResourceType.Any, 1)]
    [InlineData(ResourceType.Topic, 2)]
    [InlineData(ResourceType.Group, 3)]
    [InlineData(ResourceType.Cluster, 4)]
    [InlineData(ResourceType.TransactionalId, 5)]
    [InlineData(ResourceType.DelegationToken, 6)]
    [InlineData(ResourceType.User, 7)]
    public void ResourceType_MemberValuesAreJavaWireCodes(ResourceType value, int code) =>
        Assert.Equal(code, (int)value);

    [Theory]
    [InlineData(PatternType.Unknown, 0)]
    [InlineData(PatternType.Any, 1)]
    [InlineData(PatternType.Match, 2)]
    [InlineData(PatternType.Literal, 3)]
    [InlineData(PatternType.Prefixed, 4)]
    public void PatternType_MemberValuesAreJavaWireCodes(PatternType value, int code) =>
        Assert.Equal(code, (int)value);

    [Theory]
    [InlineData(AclPermissionType.Unknown, 0)]
    [InlineData(AclPermissionType.Any, 1)]
    [InlineData(AclPermissionType.Deny, 2)]
    [InlineData(AclPermissionType.Allow, 3)]
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
        ResourcePattern pattern = new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal);

        Assert.Equal(ResourceType.Topic, pattern.ResourceType);
        Assert.Equal("orders", pattern.Name);
        Assert.Equal(PatternType.Literal, pattern.PatternType);
        Assert.False(pattern.IsUnknown);
    }

    [Fact]
    public void ResourcePattern_WildcardResourceIsStar() =>
        Assert.Equal("*", ResourcePattern.WildcardResource);

    [Fact]
    public void ResourcePattern_NullName_Throws()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ResourcePattern(ResourceType.Topic, null!, PatternType.Literal));

        Assert.Equal("name", ex.ParamName);
    }

    [Fact]
    public void ResourcePattern_AnyResourceType_ThrowsWithJavasMessage()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new ResourcePattern(ResourceType.Any, "orders", PatternType.Literal));

        Assert.StartsWith("resourceType must not be Any", ex.Message, StringComparison.Ordinal);
        Assert.Equal("resourceType", ex.ParamName);
    }

    [Theory]
    [InlineData(PatternType.Any, "patternType must not be Any")]
    [InlineData(PatternType.Match, "patternType must not be Match")]
    public void ResourcePattern_FilterOnlyPatternType_ThrowsWithJavasMessage(
        PatternType patternType,
        string expected)
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new ResourcePattern(ResourceType.Topic, "orders", patternType));

        Assert.StartsWith(expected, ex.Message, StringComparison.Ordinal);
        Assert.Equal("patternType", ex.ParamName);
    }

    [Fact]
    public void ResourcePattern_EmptyName_IsAccepted() =>
        Assert.Equal(string.Empty, new ResourcePattern(ResourceType.Topic, string.Empty, PatternType.Literal).Name);

    [Fact]
    public void ResourcePattern_IsUnknown_TracksEitherComponent()
    {
        Assert.True(new ResourcePattern(ResourceType.Unknown, "orders", PatternType.Literal).IsUnknown);
        Assert.True(new ResourcePattern(ResourceType.Topic, "orders", PatternType.Unknown).IsUnknown);
        Assert.False(new ResourcePattern(ResourceType.Topic, "orders", PatternType.Prefixed).IsUnknown);
    }

    [Fact]
    public void ResourcePattern_ToFilter_CarriesEveryComponent()
    {
        ResourcePatternFilter filter =
            new ResourcePattern(ResourceType.Group, "g1", PatternType.Prefixed).ToFilter();

        Assert.Equal(new ResourcePatternFilter(ResourceType.Group, "g1", PatternType.Prefixed), filter);
    }

    [Fact]
    public void ResourcePattern_ToString_MatchesJava() =>
        Assert.Equal(
            "ResourcePattern(resourceType=Topic, name=orders, patternType=Literal)",
            new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal).ToString());

    [Fact]
    public void ResourcePatternFilter_AcceptsAnyAndMatchAndNullName()
    {
        ResourcePatternFilter filter = new ResourcePatternFilter(ResourceType.Any, null, PatternType.Match);

        Assert.Equal(ResourceType.Any, filter.ResourceType);
        Assert.Null(filter.Name);
        Assert.Equal(PatternType.Match, filter.PatternType);
    }

    [Fact]
    public void ResourcePatternFilter_Any_IsTheMatchEverythingFilter()
    {
        Assert.Equal(ResourceType.Any, ResourcePatternFilter.Any.ResourceType);
        Assert.Null(ResourcePatternFilter.Any.Name);
        Assert.Equal(PatternType.Any, ResourcePatternFilter.Any.PatternType);
    }

    [Fact]
    public void ResourcePatternFilter_NullName_IsNotEmptyName()
    {
        ResourcePatternFilter wildcard = new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal);
        ResourcePatternFilter empty =
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal);

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
        Assert.Equal("wildcard", byFilter[new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal)]);
        Assert.Equal(
            "empty",
            byFilter[new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal)]);
    }

    [Fact]
    public void ResourcePatternFilter_ValueEquality()
    {
        ResourcePatternFilter a = new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Prefixed);
        ResourcePatternFilter b = new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Prefixed);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.Group, "orders", PatternType.Prefixed));
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.Topic, "Orders", PatternType.Prefixed));
        Assert.NotEqual(a, new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Literal));
        Assert.False(a.Equals("not a filter"));
    }

    [Fact]
    public void ResourcePatternFilter_ToString_MatchesJava()
    {
        Assert.Equal(
            "ResourcePattern(resourceType=Any, name=<any>, patternType=Any)",
            ResourcePatternFilter.Any.ToString());
        Assert.Equal(
            "ResourcePattern(resourceType=Topic, name=, patternType=Literal)",
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal).ToString());
    }

    [Fact]
    public void AccessControlEntry_ExposesItsComponents()
    {
        AccessControlEntry entry =
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);

        Assert.Equal("User:alice", entry.Principal);
        Assert.Equal("*", entry.Host);
        Assert.Equal(AclOperation.Read, entry.Operation);
        Assert.Equal(AclPermissionType.Allow, entry.PermissionType);
        Assert.False(entry.IsUnknown);
    }

    [Fact]
    public void AccessControlEntry_NullStrings_Throw()
    {
        Assert.Equal(
            "principal",
            Assert.Throws<ArgumentNullException>(
                () => new AccessControlEntry(null!, "*", AclOperation.Read, AclPermissionType.Allow)).ParamName);

        Assert.Equal(
            "host",
            Assert.Throws<ArgumentNullException>(
                () => new AccessControlEntry("User:alice", null!, AclOperation.Read, AclPermissionType.Allow))
                .ParamName);
    }

    [Fact]
    public void AccessControlEntry_AnyOperation_ThrowsWithJavasMessage()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.Any, AclPermissionType.Allow));

        Assert.StartsWith("operation must not be Any", ex.Message, StringComparison.Ordinal);
        Assert.Equal("operation", ex.ParamName);
    }

    [Fact]
    public void AccessControlEntry_AnyPermissionType_ThrowsWithJavasMessage()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Any));

        Assert.StartsWith("permissionType must not be Any", ex.Message, StringComparison.Ordinal);
        Assert.Equal("permissionType", ex.ParamName);
    }

    [Fact]
    public void AccessControlEntry_EmptyStrings_AreAccepted()
    {
        AccessControlEntry entry = new AccessControlEntry(
            string.Empty, string.Empty, AclOperation.Describe, AclPermissionType.Deny);

        Assert.Equal(string.Empty, entry.Principal);
        Assert.Equal(string.Empty, entry.Host);
    }

    [Fact]
    public void AccessControlEntry_IsUnknown_TracksEitherEnum()
    {
        Assert.True(new AccessControlEntry("User:a", "*", AclOperation.Unknown, AclPermissionType.Allow).IsUnknown);
        Assert.True(new AccessControlEntry("User:a", "*", AclOperation.Read, AclPermissionType.Unknown).IsUnknown);
    }

    [Fact]
    public void AccessControlEntry_ToFilter_CarriesEveryComponent()
    {
        AccessControlEntryFilter filter =
            new AccessControlEntry("User:alice", "10.0.0.1", AclOperation.Write, AclPermissionType.Deny).ToFilter();

        Assert.Equal(
            new AccessControlEntryFilter("User:alice", "10.0.0.1", AclOperation.Write, AclPermissionType.Deny),
            filter);
    }

    [Fact]
    public void AccessControlEntry_ToString_MatchesJava() =>
        Assert.Equal(
            "(principal=User:alice, host=*, operation=Read, permissionType=Allow)",
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow).ToString());

    [Fact]
    public void AccessControlEntryFilter_AcceptsNullsAndAny()
    {
        AccessControlEntryFilter filter =
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any);

        Assert.Null(filter.Principal);
        Assert.Null(filter.Host);
        Assert.Equal(AclOperation.Any, filter.Operation);
        Assert.Equal(AclPermissionType.Any, filter.PermissionType);
    }

    [Fact]
    public void AccessControlEntryFilter_Any_IsTheMatchEverythingFilter() =>
        Assert.Equal(
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any),
            AccessControlEntryFilter.Any);

    [Fact]
    public void AccessControlEntryFilter_NullStrings_AreNotEmptyStrings()
    {
        AccessControlEntryFilter nullPrincipal =
            new AccessControlEntryFilter(null, "*", AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter emptyPrincipal =
            new AccessControlEntryFilter(string.Empty, "*", AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter nullHost =
            new AccessControlEntryFilter("User:a", null, AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter emptyHost =
            new AccessControlEntryFilter("User:a", string.Empty, AclOperation.Read, AclPermissionType.Allow);

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
            byFilter[new AccessControlEntryFilter(string.Empty, "*", AclOperation.Read, AclPermissionType.Allow)]);
    }

    [Fact]
    public void AccessControlEntryFilter_ValueEquality()
    {
        AccessControlEntryFilter a =
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);
        AccessControlEntryFilter b =
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, new AccessControlEntryFilter("User:bob", "*", AclOperation.Read, AclPermissionType.Allow));
        Assert.NotEqual(
            a,
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Write, AclPermissionType.Allow));
        Assert.NotEqual(
            a,
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Deny));
        Assert.False(a.Equals(42));
    }

    [Fact]
    public void AccessControlEntryFilter_ToString_RendersNullAsAny() =>
        Assert.Equal(
            "(principal=<any>, host=<any>, operation=Any, permissionType=Any)",
            AccessControlEntryFilter.Any.ToString());

    [Fact]
    public void AclBinding_IsNested_NotFlattened()
    {
        AclBinding binding = NewBinding();

        Assert.Equal("orders", binding.Pattern.Name);
        Assert.Equal(ResourceType.Topic, binding.Pattern.ResourceType);
        Assert.Equal(PatternType.Literal, binding.Pattern.PatternType);
        Assert.Equal("User:alice", binding.Entry.Principal);
        Assert.Equal("*", binding.Entry.Host);
        Assert.Equal(AclOperation.Read, binding.Entry.Operation);
        Assert.Equal(AclPermissionType.Allow, binding.Entry.PermissionType);
        Assert.False(binding.IsUnknown);
    }

    [Fact]
    public void AclBinding_NullHalves_Throw()
    {
        AccessControlEntry entry =
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow);
        ResourcePattern pattern = new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal);

        Assert.Equal("pattern", Assert.Throws<ArgumentNullException>(() => new AclBinding(null!, entry)).ParamName);
        Assert.Equal("entry", Assert.Throws<ArgumentNullException>(() => new AclBinding(pattern, null!)).ParamName);
    }

    [Fact]
    public void AclBinding_IsUnknown_PropagatesFromEitherHalf()
    {
        Assert.True(new AclBinding(
            new ResourcePattern(ResourceType.Unknown, "orders", PatternType.Literal),
            new AccessControlEntry("User:a", "*", AclOperation.Read, AclPermissionType.Allow)).IsUnknown);

        Assert.True(new AclBinding(
            new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal),
            new AccessControlEntry("User:a", "*", AclOperation.Read, AclPermissionType.Unknown)).IsUnknown);
    }

    [Fact]
    public void AclBinding_ToFilter_CarriesBothHalves()
    {
        AclBindingFilter filter = NewBinding().ToFilter();

        Assert.Equal(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Topic, "orders", PatternType.Literal),
                new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow)),
            filter);
    }

    [Fact]
    public void AclBinding_ToString_MatchesJava() =>
        Assert.Equal(
            "(pattern=ResourcePattern(resourceType=Topic, name=orders, patternType=Literal), "
                + "entry=(principal=User:alice, host=*, operation=Read, permissionType=Allow))",
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
            new ResourcePattern(ResourceType.Topic, "payments", PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

        Assert.False(byBinding.ContainsKey(other));
        Assert.NotEqual(NewBinding(), other);
        Assert.False(NewBinding().Equals("not a binding"));
    }

    [Fact]
    public void AclBindingFilter_Any_IsBothHalvesAny()
    {
        Assert.Equal(ResourcePatternFilter.Any, AclBindingFilter.Any.PatternFilter);
        Assert.Equal(AccessControlEntryFilter.Any, AclBindingFilter.Any.EntryFilter);
        Assert.Equal(
            new AclBindingFilter(ResourcePatternFilter.Any, AccessControlEntryFilter.Any),
            AclBindingFilter.Any);
    }

    [Fact]
    public void AclBindingFilter_NullHalves_Throw()
    {
        Assert.Equal(
            "patternFilter",
            Assert.Throws<ArgumentNullException>(
                () => new AclBindingFilter(null!, AccessControlEntryFilter.Any)).ParamName);

        Assert.Equal(
            "entryFilter",
            Assert.Throws<ArgumentNullException>(
                () => new AclBindingFilter(ResourcePatternFilter.Any, null!)).ParamName);
    }

    [Fact]
    public void AclBindingFilter_IsUnknown_PropagatesFromEitherHalf()
    {
        Assert.False(AclBindingFilter.Any.IsUnknown);

        Assert.True(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Unknown, null, PatternType.Any),
            AccessControlEntryFilter.Any).IsUnknown);

        Assert.True(new AclBindingFilter(
            ResourcePatternFilter.Any,
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Unknown)).IsUnknown);
    }

    [Fact]
    public void AclBindingFilter_IsUsableAsADictionaryKey_AndKeepsNullApartFromEmpty()
    {
        AclBindingFilter wildcardName = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal),
            AccessControlEntryFilter.Any);
        AclBindingFilter emptyName = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal),
            AccessControlEntryFilter.Any);

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
                new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal),
                AccessControlEntryFilter.Any)]);
        Assert.Equal(
            "empty",
            byFilter[new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal),
                AccessControlEntryFilter.Any)]);
        Assert.False(wildcardName.Equals("not a filter"));
    }

    [Fact]
    public void AclBindingFilter_ToString_MatchesJava() =>
        Assert.Equal(
            "(patternFilter=ResourcePattern(resourceType=Any, name=<any>, patternType=Any), "
                + "entryFilter=(principal=<any>, host=<any>, operation=Any, permissionType=Any))",
            AclBindingFilter.Any.ToString());

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
            new ResourcePattern(ResourceType.Topic, "orders", PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
