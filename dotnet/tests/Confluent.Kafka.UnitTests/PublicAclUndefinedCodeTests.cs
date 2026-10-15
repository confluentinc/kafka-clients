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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M15/P13.3 F8 — the four ACL value-type constructors store an enum value that is not a
/// defined member as that enum's <c>Unknown</c>, as Java's <c>fromCode</c> maps a code it has
/// no member for (<c>AclOperation.java:151</c>, <c>AclPermissionType.java:74</c>,
/// <c>ResourceType.java:94</c>, <c>PatternType.java:111</c>). One test per enum per
/// constructor.
/// </summary>
/// <remarks>
/// <para>
/// Each test asserts three things, because each is what a different consumer relies on: the
/// stored property (what the caller reads back), <c>IsUnknown</c> (Java's <c>isUnknown()</c>,
/// true for a <c>fromCode</c>-mapped value), and value equality <em>plus</em> an equal hash
/// with the instance built from <c>Unknown</c> directly. The last one is what
/// <c>DistinctBindings</c> / <c>DistinctFilters</c> and every result dictionary key on, so two
/// inputs differing only in undefined codes are one key — as the core, which reads each code
/// through <c>fromCode</c>, answers them once.
/// </para>
/// <para>
/// The codes cover both of the core's fallback routes: <c>-1</c>, <c>16</c> and <c>99</c>
/// fit an <c>int8_t</c> and are undefined codes, while <c>128</c> and <c>259</c> do not fit
/// one at all. Every code is undefined for all four enums (<see cref="AclOperation"/>'s
/// largest member is 15).
/// </para>
/// <para>Pure managed state: no native handle, no broker.</para>
/// </remarks>
public sealed class PublicAclUndefinedCodeTests
{
    // ---- AccessControlEntry ----

    /// <summary><see cref="AccessControlEntry"/> stores an undefined operation as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    /// <summary><see cref="AccessControlEntry"/> stores an undefined permission type as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    // ---- AccessControlEntryFilter ----

    /// <summary><see cref="AccessControlEntryFilter"/> stores an undefined operation as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    /// <summary><see cref="AccessControlEntryFilter"/> stores an undefined permission type as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    // ---- ResourcePattern ----

    /// <summary><see cref="ResourcePattern"/> stores an undefined resource type as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    /// <summary><see cref="ResourcePattern"/> stores an undefined pattern type as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    // ---- ResourcePatternFilter ----

    /// <summary><see cref="ResourcePatternFilter"/> stores an undefined resource type as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    /// <summary><see cref="ResourcePatternFilter"/> stores an undefined pattern type as <c>Unknown</c>.</summary>
    /// <param name="code">An undefined code.</param>
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

    // ---- what the normalization must NOT touch ----

    /// <summary>
    /// ⚠ The sentinels are defined members and are kept: a filter still holds <c>Any</c> and
    /// <c>Match</c>, and a concrete type still rejects them with Java's messages — the fold
    /// runs before those checks, so an undefined value can never reach them and a defined one
    /// is never changed by it.
    /// </summary>
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
