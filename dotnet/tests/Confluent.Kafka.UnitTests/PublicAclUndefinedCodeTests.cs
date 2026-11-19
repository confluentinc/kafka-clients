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

using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Resource;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks that the four ACL value-type constructors store an undefined enum value as that enum's <c>UNKNOWN</c>, as
/// Java's <c>fromCode</c> does, equal and with an equal hash to one built from <c>UNKNOWN</c>, for codes that fit an
/// <c>int8_t</c> and codes that do not (one whose low byte is a member's code included); and that the <c>ANY</c> /
/// <c>MATCH</c> sentinels are kept, and still rejected where Java rejects them.
/// </summary>
public sealed class PublicAclUndefinedCodeTests
{
    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntry_StoresAnUndefinedOperation_AsUnknown(int code)
    {
        AccessControlEntry entry = new AccessControlEntry(
            "User:alice", "*", (AclOperation)code, AclPermissionType.ALLOW);
        AccessControlEntry unknown = new AccessControlEntry(
            "User:alice", "*", AclOperation.UNKNOWN, AclPermissionType.ALLOW);

        Assert.Equal(AclOperation.UNKNOWN, entry.Operation());
        Assert.True(entry.IsUnknown());
        Assert.Equal(unknown, entry);
        Assert.Equal(unknown.GetHashCode(), entry.GetHashCode());
    }

    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntry_StoresAnUndefinedPermissionType_AsUnknown(int code)
    {
        AccessControlEntry entry = new AccessControlEntry(
            "User:alice", "*", AclOperation.READ, (AclPermissionType)code);
        AccessControlEntry unknown = new AccessControlEntry(
            "User:alice", "*", AclOperation.READ, AclPermissionType.UNKNOWN);

        Assert.Equal(AclPermissionType.UNKNOWN, entry.PermissionType());
        Assert.True(entry.IsUnknown());
        Assert.Equal(unknown, entry);
        Assert.Equal(unknown.GetHashCode(), entry.GetHashCode());
    }

    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntryFilter_StoresAnUndefinedOperation_AsUnknown(int code)
    {
        AccessControlEntryFilter filter = new AccessControlEntryFilter(
            null, null, (AclOperation)code, AclPermissionType.ANY);
        AccessControlEntryFilter unknown = new AccessControlEntryFilter(
            null, null, AclOperation.UNKNOWN, AclPermissionType.ANY);

        Assert.Equal(AclOperation.UNKNOWN, filter.Operation());
        Assert.True(filter.IsUnknown());
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntryFilter_StoresAnUndefinedPermissionType_AsUnknown(int code)
    {
        AccessControlEntryFilter filter = new AccessControlEntryFilter(
            null, null, AclOperation.ANY, (AclPermissionType)code);
        AccessControlEntryFilter unknown = new AccessControlEntryFilter(
            null, null, AclOperation.ANY, AclPermissionType.UNKNOWN);

        Assert.Equal(AclPermissionType.UNKNOWN, filter.PermissionType());
        Assert.True(filter.IsUnknown());
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePattern_StoresAnUndefinedResourceType_AsUnknown(int code)
    {
        ResourcePattern pattern = new ResourcePattern((ResourceType)code, "t", PatternType.LITERAL);
        ResourcePattern unknown = new ResourcePattern(ResourceType.UNKNOWN, "t", PatternType.LITERAL);

        Assert.Equal(ResourceType.UNKNOWN, pattern.ResourceType());
        Assert.True(pattern.IsUnknown());
        Assert.Equal(unknown, pattern);
        Assert.Equal(unknown.GetHashCode(), pattern.GetHashCode());
    }

    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePattern_StoresAnUndefinedPatternType_AsUnknown(int code)
    {
        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, "t", (PatternType)code);
        ResourcePattern unknown = new ResourcePattern(ResourceType.TOPIC, "t", PatternType.UNKNOWN);

        Assert.Equal(PatternType.UNKNOWN, pattern.PatternType());
        Assert.True(pattern.IsUnknown());
        Assert.Equal(unknown, pattern);
        Assert.Equal(unknown.GetHashCode(), pattern.GetHashCode());
    }

    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePatternFilter_StoresAnUndefinedResourceType_AsUnknown(int code)
    {
        ResourcePatternFilter filter = new ResourcePatternFilter((ResourceType)code, null, PatternType.ANY);
        ResourcePatternFilter unknown = new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.ANY);

        Assert.Equal(ResourceType.UNKNOWN, filter.ResourceType());
        Assert.True(filter.IsUnknown());
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Theory]
    [InlineData(-253)]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePatternFilter_StoresAnUndefinedPatternType_AsUnknown(int code)
    {
        ResourcePatternFilter filter = new ResourcePatternFilter(ResourceType.ANY, null, (PatternType)code);
        ResourcePatternFilter unknown = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.UNKNOWN);

        Assert.Equal(PatternType.UNKNOWN, filter.PatternType());
        Assert.True(filter.IsUnknown());
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Fact]
    public void TheSentinels_AreKept_AndStillRejectedWhereJavaRejectsThem()
    {
        ResourcePatternFilter match = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.MATCH);
        Assert.Equal(ResourceType.ANY, match.ResourceType());
        Assert.Equal(PatternType.MATCH, match.PatternType());

        AccessControlEntryFilter any = new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);
        Assert.Equal(AclOperation.ANY, any.Operation());
        Assert.Equal(AclPermissionType.ANY, any.PermissionType());

        IllegalArgumentException operation = Assert.Throws<IllegalArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.ANY, AclPermissionType.ALLOW));
        Assert.Equal("operation must not be ANY", operation.Message);

        IllegalArgumentException resourceType = Assert.Throws<IllegalArgumentException>(
            () => new ResourcePattern(ResourceType.ANY, "t", PatternType.LITERAL));
        Assert.Equal("resourceType must not be ANY", resourceType.Message);
    }
}
