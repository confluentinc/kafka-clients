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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Sends <c>createAcls</c> and <c>deleteAcls</c> inputs carrying an undefined ACL enum code to the core mock, and
/// checks that each result is keyed by the caller's own binding or filter, read back as <c>UNKNOWN</c>.
/// </summary>
public sealed class AdminAclUndefinedCodeKeyingTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implemented yet";

    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task CreateAcls_AnUndefinedCode_KeysByTheCallersBinding_ReadBackAsUnknown(Column column)
    {
        AclBinding binding = Binding(column, 99);
        Assert.Equal(0, Code(column, binding.Pattern.ResourceType, binding.Pattern.PatternType, binding.Entry.Operation, binding.Entry.PermissionType));

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateAclsResult result = AsyncCreateAclsResult.OfClient(admin.CreateAcls(new[] { binding }, options: null));

        AclBinding key = Assert.Single(result.Values).Key;
        Assert.Equal(binding, key);
        Assert.Equal(0, Code(column, key.Pattern.ResourceType, key.Pattern.PatternType, key.Entry.Operation, key.Entry.PermissionType));
        await AssertNotImplemented(result.Values[binding].Get());
    }

    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task CreateAcls_TwoBindingsDifferingOnlyInUndefinedCodes_AreOneKey_AndComplete(Column column)
    {
        AclBinding first = Binding(column, 98);
        AclBinding second = Binding(column, 99);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateAclsResult result = AsyncCreateAclsResult.OfClient(admin.CreateAcls(new[] { first, second }, options: null));

        Assert.Equal(first, Assert.Single(result.Values).Key);
        await AssertNotImplemented(result.Values[second].Get());
    }

    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task DeleteAcls_AnUndefinedCode_KeysByTheCallersFilter_ReadBackAsUnknown(Column column)
    {
        AclBindingFilter filter = Filter(column, 99);
        Assert.Equal(0, Code(column, filter.PatternFilter.ResourceType, filter.PatternFilter.PatternType, filter.EntryFilter.Operation, filter.EntryFilter.PermissionType));

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDeleteAclsResult result = AsyncDeleteAclsResult.OfClient(admin.DeleteAcls(new[] { filter }, options: null));

        AclBindingFilter key = Assert.Single(result.Values).Key;
        Assert.Equal(filter, key);
        Assert.Equal(0, Code(column, key.PatternFilter.ResourceType, key.PatternFilter.PatternType, key.EntryFilter.Operation, key.EntryFilter.PermissionType));
        await AssertNotImplemented(result.Values[filter].Get());
    }

    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task DeleteAcls_TwoFiltersDifferingOnlyInUndefinedCodes_AreOneKey_AndComplete(Column column)
    {
        AclBindingFilter first = Filter(column, 98);
        AclBindingFilter second = Filter(column, 99);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDeleteAclsResult result = AsyncDeleteAclsResult.OfClient(admin.DeleteAcls(new[] { first, second }, options: null));

        Assert.Equal(first, Assert.Single(result.Values).Key);
        await AssertNotImplemented(result.Values[second].Get());
    }

    public enum Column
    {
        ResourceType,
        PatternType,
        Operation,
        PermissionType,
    }

    private static async Task AssertNotImplemented(Task outcome)
    {
        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(() => TestTimeout.Run(() => outcome, s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    private static int Code(
        Column column, ResourceType resourceType, PatternType patternType, AclOperation operation, AclPermissionType permissionType) =>
        column switch
        {
            Column.ResourceType => (int)resourceType,
            Column.PatternType => (int)patternType,
            Column.Operation => (int)operation,
            _ => (int)permissionType,
        };

    private static AclBinding Binding(Column column, int code) =>
        new AclBinding(
            new ResourcePattern(
                column == Column.ResourceType ? (ResourceType)code : ResourceType.Topic,
                "undefined-code-acl",
                column == Column.PatternType ? (PatternType)code : PatternType.Literal),
            new AccessControlEntry(
                "User:alice",
                "*",
                column == Column.Operation ? (AclOperation)code : AclOperation.Read,
                column == Column.PermissionType ? (AclPermissionType)code : AclPermissionType.Allow));

    private static AclBindingFilter Filter(Column column, int code) =>
        new AclBindingFilter(
            new ResourcePatternFilter(
                column == Column.ResourceType ? (ResourceType)code : ResourceType.Topic,
                "undefined-code-acl",
                column == Column.PatternType ? (PatternType)code : PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice",
                "*",
                column == Column.Operation ? (AclOperation)code : AclOperation.Read,
                column == Column.PermissionType ? (AclPermissionType)code : AclPermissionType.Allow));
}
