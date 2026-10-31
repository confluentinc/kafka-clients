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
/// Checks that the four ACL value-type constructors store an undefined enum value as that enum's <c>Unknown</c>, as
/// Java's <c>fromCode</c> does, equal and with an equal hash to one built from <c>Unknown</c>, for codes that fit an
/// <c>int8_t</c> and codes that do not; and that the <c>Any</c> / <c>Match</c> sentinels are kept, and still rejected
/// where Java rejects them.
/// </summary>
public sealed class PublicAclUndefinedCodeTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntry_StoresAnUndefinedOperation_AsUnknown(int code)
    {
        AccessControlEntry entry = new AccessControlEntry(
            "User:alice", "*", (AclOperation)code, AclPermissionType.Allow);
        AccessControlEntry unknown = new AccessControlEntry(
            "User:alice", "*", AclOperation.Unknown, AclPermissionType.Allow);

        Assert.Equal(AclOperation.Unknown, entry.Operation);
        Assert.True(entry.IsUnknown);
        Assert.Equal(unknown, entry);
        Assert.Equal(unknown.GetHashCode(), entry.GetHashCode());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntry_StoresAnUndefinedPermissionType_AsUnknown(int code)
    {
        AccessControlEntry entry = new AccessControlEntry(
            "User:alice", "*", AclOperation.Read, (AclPermissionType)code);
        AccessControlEntry unknown = new AccessControlEntry(
            "User:alice", "*", AclOperation.Read, AclPermissionType.Unknown);

        Assert.Equal(AclPermissionType.Unknown, entry.PermissionType);
        Assert.True(entry.IsUnknown);
        Assert.Equal(unknown, entry);
        Assert.Equal(unknown.GetHashCode(), entry.GetHashCode());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntryFilter_StoresAnUndefinedOperation_AsUnknown(int code)
    {
        AccessControlEntryFilter filter = new AccessControlEntryFilter(
            null, null, (AclOperation)code, AclPermissionType.Any);
        AccessControlEntryFilter unknown = new AccessControlEntryFilter(
            null, null, AclOperation.Unknown, AclPermissionType.Any);

        Assert.Equal(AclOperation.Unknown, filter.Operation);
        Assert.True(filter.IsUnknown);
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void AccessControlEntryFilter_StoresAnUndefinedPermissionType_AsUnknown(int code)
    {
        AccessControlEntryFilter filter = new AccessControlEntryFilter(
            null, null, AclOperation.Any, (AclPermissionType)code);
        AccessControlEntryFilter unknown = new AccessControlEntryFilter(
            null, null, AclOperation.Any, AclPermissionType.Unknown);

        Assert.Equal(AclPermissionType.Unknown, filter.PermissionType);
        Assert.True(filter.IsUnknown);
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePattern_StoresAnUndefinedResourceType_AsUnknown(int code)
    {
        ResourcePattern pattern = new ResourcePattern((ResourceType)code, "t", PatternType.Literal);
        ResourcePattern unknown = new ResourcePattern(ResourceType.Unknown, "t", PatternType.Literal);

        Assert.Equal(ResourceType.Unknown, pattern.ResourceType);
        Assert.True(pattern.IsUnknown);
        Assert.Equal(unknown, pattern);
        Assert.Equal(unknown.GetHashCode(), pattern.GetHashCode());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePattern_StoresAnUndefinedPatternType_AsUnknown(int code)
    {
        ResourcePattern pattern = new ResourcePattern(ResourceType.Topic, "t", (PatternType)code);
        ResourcePattern unknown = new ResourcePattern(ResourceType.Topic, "t", PatternType.Unknown);

        Assert.Equal(PatternType.Unknown, pattern.PatternType);
        Assert.True(pattern.IsUnknown);
        Assert.Equal(unknown, pattern);
        Assert.Equal(unknown.GetHashCode(), pattern.GetHashCode());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePatternFilter_StoresAnUndefinedResourceType_AsUnknown(int code)
    {
        ResourcePatternFilter filter = new ResourcePatternFilter((ResourceType)code, null, PatternType.Any);
        ResourcePatternFilter unknown = new ResourcePatternFilter(ResourceType.Unknown, null, PatternType.Any);

        Assert.Equal(ResourceType.Unknown, filter.ResourceType);
        Assert.True(filter.IsUnknown);
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(16)]
    [InlineData(99)]
    [InlineData(128)]
    [InlineData(259)]
    public void ResourcePatternFilter_StoresAnUndefinedPatternType_AsUnknown(int code)
    {
        ResourcePatternFilter filter = new ResourcePatternFilter(ResourceType.Any, null, (PatternType)code);
        ResourcePatternFilter unknown = new ResourcePatternFilter(ResourceType.Any, null, PatternType.Unknown);

        Assert.Equal(PatternType.Unknown, filter.PatternType);
        Assert.True(filter.IsUnknown);
        Assert.Equal(unknown, filter);
        Assert.Equal(unknown.GetHashCode(), filter.GetHashCode());
    }

    [Fact]
    public void TheSentinels_AreKept_AndStillRejectedWhereJavaRejectsThem()
    {
        ResourcePatternFilter match = new ResourcePatternFilter(ResourceType.Any, null, PatternType.Match);
        Assert.Equal(ResourceType.Any, match.ResourceType);
        Assert.Equal(PatternType.Match, match.PatternType);

        AccessControlEntryFilter any = new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any);
        Assert.Equal(AclOperation.Any, any.Operation);
        Assert.Equal(AclPermissionType.Any, any.PermissionType);

        ArgumentException operation = Assert.Throws<ArgumentException>(
            () => new AccessControlEntry("User:alice", "*", AclOperation.Any, AclPermissionType.Allow));
        Assert.Equal("operation", operation.ParamName);
        Assert.StartsWith("operation must not be Any", operation.Message, StringComparison.Ordinal);

        ArgumentException resourceType = Assert.Throws<ArgumentException>(
            () => new ResourcePattern(ResourceType.Any, "t", PatternType.Literal));
        Assert.Equal("resourceType", resourceType.ParamName);
        Assert.StartsWith("resourceType must not be Any", resourceType.Message, StringComparison.Ordinal);
    }
}
