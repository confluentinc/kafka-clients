// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Resource;

using Xunit;

using static Confluent.Kafka.Common.Resource.PatternType;
using static Confluent.Kafka.Common.Resource.ResourceType;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Matches</c>, <c>MatchesAtMostOne</c> and <c>FindIndefiniteField</c> on the ACL filters: Java's
/// <c>ResourcePatternFilterTest</c> and the matching cases of <c>AclBindingTest</c>.
/// </summary>
public sealed class PublicAclFilterMatchingTests
{
    private static readonly AclBinding s_acl1 = new AclBinding(
        new ResourcePattern(TOPIC, "mytopic", LITERAL),
        new AccessControlEntry("User:ANONYMOUS", string.Empty, AclOperation.ALL, AclPermissionType.ALLOW));

    private static readonly AclBinding s_acl2 = new AclBinding(
        new ResourcePattern(TOPIC, "mytopic", LITERAL),
        new AccessControlEntry("User:*", string.Empty, AclOperation.READ, AclPermissionType.ALLOW));

    private static readonly AclBinding s_acl3 = new AclBinding(
        new ResourcePattern(TOPIC, "mytopic2", LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.READ, AclPermissionType.DENY));

    private static readonly AclBinding s_unknownAcl = new AclBinding(
        new ResourcePattern(TOPIC, "mytopic2", LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "127.0.0.1", AclOperation.UNKNOWN, AclPermissionType.DENY));

    private static readonly AclBindingFilter s_anyAnonymous = new AclBindingFilter(
        ResourcePatternFilter.ANY,
        new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY));

    private static readonly AclBindingFilter s_anyDeny = new AclBindingFilter(
        ResourcePatternFilter.ANY,
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.DENY));

    private static readonly AclBindingFilter s_anyMytopic = new AclBindingFilter(
        new ResourcePatternFilter(TOPIC, "mytopic", LITERAL),
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

    [Fact]
    public void ShouldBeUnknownIfResourceTypeUnknown() =>
        Assert.True(new ResourcePatternFilter(ResourceType.UNKNOWN, null, LITERAL).IsUnknown);

    [Fact]
    public void ShouldBeUnknownIfPatternTypeUnknown() =>
        Assert.True(new ResourcePatternFilter(GROUP, null, PatternType.UNKNOWN).IsUnknown);

    [Fact]
    public void ShouldNotMatchIfDifferentResourceType() =>
        Assert.False(new ResourcePatternFilter(TOPIC, "Name", LITERAL).Matches(new ResourcePattern(GROUP, "Name", LITERAL)));

    [Fact]
    public void ShouldNotMatchIfDifferentName() =>
        Assert.False(
            new ResourcePatternFilter(TOPIC, "Different", PREFIXED).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldNotMatchIfDifferentNameCase() =>
        Assert.False(new ResourcePatternFilter(TOPIC, "NAME", LITERAL).Matches(new ResourcePattern(TOPIC, "Name", LITERAL)));

    [Fact]
    public void ShouldNotMatchIfDifferentPatternType() =>
        Assert.False(new ResourcePatternFilter(TOPIC, "Name", LITERAL).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldMatchWhereResourceTypeIsAny() =>
        Assert.True(
            new ResourcePatternFilter(ResourceType.ANY, "Name", PREFIXED).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldMatchWhereResourceNameIsAny() =>
        Assert.True(new ResourcePatternFilter(TOPIC, null, PREFIXED).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldMatchWherePatternTypeIsAny() =>
        Assert.True(
            new ResourcePatternFilter(TOPIC, null, PatternType.ANY).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldMatchWherePatternTypeIsMatch() =>
        Assert.True(new ResourcePatternFilter(TOPIC, null, MATCH).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldMatchLiteralIfExactMatch() =>
        Assert.True(new ResourcePatternFilter(TOPIC, "Name", LITERAL).Matches(new ResourcePattern(TOPIC, "Name", LITERAL)));

    [Fact]
    public void ShouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeAny() =>
        Assert.True(
            new ResourcePatternFilter(TOPIC, "Name", PatternType.ANY).Matches(new ResourcePattern(TOPIC, "Name", LITERAL)));

    [Fact]
    public void ShouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeMatch() =>
        Assert.True(new ResourcePatternFilter(TOPIC, "Name", MATCH).Matches(new ResourcePattern(TOPIC, "Name", LITERAL)));

    [Fact]
    public void ShouldNotMatchLiteralIfNamePrefixed() =>
        Assert.False(
            new ResourcePatternFilter(TOPIC, "Name-something", MATCH).Matches(new ResourcePattern(TOPIC, "Name", LITERAL)));

    [Fact]
    public void ShouldMatchLiteralWildcardIfExactMatch() =>
        Assert.True(new ResourcePatternFilter(TOPIC, "*", LITERAL).Matches(new ResourcePattern(TOPIC, "*", LITERAL)));

    [Fact]
    public void ShouldNotMatchLiteralWildcardAgainstOtherName() =>
        Assert.False(new ResourcePatternFilter(TOPIC, "Name", LITERAL).Matches(new ResourcePattern(TOPIC, "*", LITERAL)));

    [Fact]
    public void ShouldNotMatchLiteralWildcardTheWayAround() =>
        Assert.False(new ResourcePatternFilter(TOPIC, "*", LITERAL).Matches(new ResourcePattern(TOPIC, "Name", LITERAL)));

    [Fact]
    public void ShouldNotMatchLiteralWildcardIfFilterHasPatternTypeOfAny() =>
        Assert.False(
            new ResourcePatternFilter(TOPIC, "Name", PatternType.ANY).Matches(new ResourcePattern(TOPIC, "*", LITERAL)));

    [Fact]
    public void ShouldMatchLiteralWildcardIfFilterHasPatternTypeOfMatch() =>
        Assert.True(new ResourcePatternFilter(TOPIC, "Name", MATCH).Matches(new ResourcePattern(TOPIC, "*", LITERAL)));

    [Fact]
    public void ShouldMatchPrefixedIfExactMatch() =>
        Assert.True(new ResourcePatternFilter(TOPIC, "Name", PREFIXED).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldNotMatchIfBothPrefixedAndFilterIsPrefixOfResource() =>
        Assert.False(
            new ResourcePatternFilter(TOPIC, "Name", PREFIXED).Matches(new ResourcePattern(TOPIC, "Name-something", PREFIXED)));

    [Fact]
    public void ShouldNotMatchIfBothPrefixedAndResourceIsPrefixOfFilter() =>
        Assert.False(
            new ResourcePatternFilter(TOPIC, "Name-something", PREFIXED).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldNotMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny() =>
        Assert.False(
            new ResourcePatternFilter(TOPIC, "Name-something", PatternType.ANY)
                .Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void ShouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsMatch() =>
        Assert.True(
            new ResourcePatternFilter(TOPIC, "Name-something", MATCH).Matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));

    [Fact]
    public void AclBinding_TestMatching()
    {
        Assert.True(AclBindingFilter.ANY.Matches(s_acl1));
        Assert.True(AclBindingFilter.ANY.Matches(s_acl2));
        Assert.True(AclBindingFilter.ANY.Matches(s_acl3));
        Assert.True(s_anyAnonymous.Matches(s_acl1));
        Assert.False(s_anyAnonymous.Matches(s_acl2));
        Assert.True(s_anyAnonymous.Matches(s_acl3));
        Assert.False(s_anyDeny.Matches(s_acl1));
        Assert.False(s_anyDeny.Matches(s_acl2));
        Assert.True(s_anyDeny.Matches(s_acl3));
        Assert.True(s_anyMytopic.Matches(s_acl1));
        Assert.True(s_anyMytopic.Matches(s_acl2));
        Assert.False(s_anyMytopic.Matches(s_acl3));
        Assert.True(s_anyAnonymous.Matches(s_unknownAcl));
        Assert.True(s_anyDeny.Matches(s_unknownAcl));
        Assert.False(s_anyMytopic.Matches(s_unknownAcl));
    }

    [Fact]
    public void AclBinding_TestMatchesAtMostOne()
    {
        Assert.Null(s_acl1.ToFilter().FindIndefiniteField());
        Assert.Null(s_acl2.ToFilter().FindIndefiniteField());
        Assert.Null(s_acl3.ToFilter().FindIndefiniteField());
        Assert.True(s_acl1.ToFilter().MatchesAtMostOne());
        Assert.False(s_anyAnonymous.MatchesAtMostOne());
        Assert.False(s_anyDeny.MatchesAtMostOne());
        Assert.False(s_anyMytopic.MatchesAtMostOne());
    }

    [Fact]
    public void ResourcePatternFilter_FindIndefiniteField_NamesTheFirstIndefiniteComponent_InJavasWords()
    {
        Assert.Equal("Resource type is ANY.", new ResourcePatternFilter(ResourceType.ANY, null, MATCH).FindIndefiniteField());
        Assert.Equal(
            "Resource type is UNKNOWN.", new ResourcePatternFilter(ResourceType.UNKNOWN, "n", LITERAL).FindIndefiniteField());
        Assert.Equal("Resource name is NULL.", new ResourcePatternFilter(TOPIC, null, LITERAL).FindIndefiniteField());
        Assert.Equal("Resource pattern type is MATCH.", new ResourcePatternFilter(TOPIC, "n", MATCH).FindIndefiniteField());
        Assert.Equal(
            "Resource pattern type is UNKNOWN.",
            new ResourcePatternFilter(TOPIC, "n", PatternType.UNKNOWN).FindIndefiniteField());
        Assert.Null(new ResourcePatternFilter(TOPIC, "n", PREFIXED).FindIndefiniteField());
        Assert.True(new ResourcePatternFilter(TOPIC, "n", PREFIXED).MatchesAtMostOne());
        Assert.False(ResourcePatternFilter.ANY.MatchesAtMostOne());
    }

    [Fact]
    public void AccessControlEntryFilter_FindIndefiniteField_NamesTheFirstIndefiniteComponent_InJavasWords()
    {
        Assert.Equal("Principal is NULL", AccessControlEntryFilter.ANY.FindIndefiniteField());
        Assert.Equal("Host is NULL", Entry("p", null, AclOperation.ANY, AclPermissionType.ANY).FindIndefiniteField());
        Assert.Equal("Operation is ANY", Entry("p", "h", AclOperation.ANY, AclPermissionType.ANY).FindIndefiniteField());
        Assert.Equal(
            "Operation is UNKNOWN", Entry("p", "h", AclOperation.UNKNOWN, AclPermissionType.ANY).FindIndefiniteField());
        Assert.Equal(
            "Permission type is ANY", Entry("p", "h", AclOperation.READ, AclPermissionType.ANY).FindIndefiniteField());
        Assert.Equal(
            "Permission type is UNKNOWN", Entry("p", "h", AclOperation.READ, AclPermissionType.UNKNOWN).FindIndefiniteField());
        Assert.Null(Entry("p", "h", AclOperation.READ, AclPermissionType.ALLOW).FindIndefiniteField());
        Assert.True(Entry("p", string.Empty, AclOperation.READ, AclPermissionType.ALLOW).MatchesAtMostOne());
    }

    [Fact]
    public void AclBindingFilter_FindIndefiniteField_ReadsThePatternHalfFirst()
    {
        AclBindingFilter both = new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY);
        AclBindingFilter entryOnly = new AclBindingFilter(
            new ResourcePatternFilter(TOPIC, "t", LITERAL), AccessControlEntryFilter.ANY);

        Assert.Equal("Resource type is ANY.", both.FindIndefiniteField());
        Assert.Equal("Principal is NULL", entryOnly.FindIndefiniteField());
        Assert.False(entryOnly.MatchesAtMostOne());
    }

    [Fact]
    public void AccessControlEntryFilter_Matches_ComparesNamesCaseSensitively()
    {
        AccessControlEntry entry = new AccessControlEntry("User:alice", "h", AclOperation.READ, AclPermissionType.ALLOW);

        Assert.True(Entry("User:alice", "h", AclOperation.READ, AclPermissionType.ALLOW).Matches(entry));
        Assert.False(Entry("User:Alice", null, AclOperation.ANY, AclPermissionType.ANY).Matches(entry));
        Assert.False(Entry(null, "H", AclOperation.ANY, AclPermissionType.ANY).Matches(entry));
        Assert.False(Entry(null, null, AclOperation.WRITE, AclPermissionType.ANY).Matches(entry));
        Assert.False(Entry(null, null, AclOperation.ANY, AclPermissionType.DENY).Matches(entry));
    }

    [Fact]
    public void Matches_RefusesANullArgument()
    {
        Assert.Equal("pattern", Assert.Throws<ArgumentNullException>(() => ResourcePatternFilter.ANY.Matches(null!)).ParamName);
        Assert.Equal("other", Assert.Throws<ArgumentNullException>(() => AccessControlEntryFilter.ANY.Matches(null!)).ParamName);
        Assert.Equal("binding", Assert.Throws<ArgumentNullException>(() => AclBindingFilter.ANY.Matches(null!)).ParamName);
    }

    [Fact]
    public void ResourcePatternFilter_Matches_AMatchFilterAgainstAnUnknownPatternType_ThrowsJavasError()
    {
        IllegalArgumentException thrown = Assert.Throws<IllegalArgumentException>(
            () => new ResourcePatternFilter(TOPIC, "Name", MATCH).Matches(new ResourcePattern(TOPIC, "Name", PatternType.UNKNOWN)));

        Assert.Equal("Unsupported PatternType: UNKNOWN", thrown.Message);
    }

    private static AccessControlEntryFilter Entry(
        string? principal, string? host, AclOperation operation, AclPermissionType permissionType) =>
        new AccessControlEntryFilter(principal, host, operation, permissionType);
}
