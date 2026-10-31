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

using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Resource;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>CreateAcls</c> end to end against the mocks, which fail every binding with the mock's own answer, as Java's mock
/// does: per-binding results keyed by <see cref="AclBinding"/> value, <c>All()</c>, the preconditions and the options.
/// </summary>
public sealed class PublicAdminCreateAclsTests
{
    private const string NotImplemented = "Not implemented yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Values_AreKeyedByValue_NotByReference()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateAclsResult result = admin.CreateAcls(new[] { Binding("keyed-by-value") });

        AclBinding lookup = Binding("keyed-by-value");
        Assert.NotSame(lookup, Assert.Single(result.Values).Key);
        Assert.True(result.Values.ContainsKey(lookup));

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[lookup].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task EachBinding_GetsItsOwnTask_AndItsKeyRoundTrips()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBinding alpha = new AclBinding(
            new ResourcePattern(ResourceType.Topic, "rt-alpha", PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
        AclBinding beta = new AclBinding(
            new ResourcePattern(ResourceType.Group, "rt-beta", PatternType.Prefixed),
            new AccessControlEntry(
                "User:bob", "10.0.0.7", AclOperation.DescribeConfigs, AclPermissionType.Deny));

        AsyncCreateAclsResult result = admin.CreateAcls(new[] { alpha, beta });

        Assert.Equal(2, result.Values.Count);
        Assert.NotSame(result.Values[alpha].Get(), result.Values[beta].Get());

        Assert.Contains(alpha, result.Values.Keys);
        Assert.Contains(beta, result.Values.Keys);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[alpha].Get(), s_deadline));
        await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[beta].Get(), s_deadline));
    }

    [Fact]
    public async Task OneKey_IsAwaitableWithoutTheOthers()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBinding first = Binding("solo-first");
        AsyncCreateAclsResult result = admin.CreateAcls(new[] { first, Binding("solo-second") });

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values[first].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);

        // Observe the other one too, so no task is left with an unobserved fault.
        await Assert.ThrowsAsync<NotSupportedException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task All_FaultsWhenAnyBindingFails()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateAclsResult result = admin.CreateAcls(new[] { Binding("all-a"), Binding("all-b") });

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.All().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task All_OverAnEmptyRequest_Completes()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateAclsResult result = admin.CreateAcls(Array.Empty<AclBinding>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task DuplicateBindings_CollapseToOneEntry()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateAclsResult result = admin.CreateAcls(new[] { Binding("dup"), Binding("dup") });

        Assert.Single(result.Values);
        await Assert.ThrowsAsync<NotSupportedException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task NonAsciiName_RoundTripsThroughTheResultKey()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBinding binding = Binding("tópico-café-日本");
        AsyncCreateAclsResult result = admin.CreateAcls(new[] { binding });

        AclBinding key = Assert.Single(result.Values).Key;
        Assert.Equal("tópico-café-日本", key.Pattern.Name);
        Assert.Equal(binding, key);

        await Assert.ThrowsAsync<NotSupportedException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "acls",
            Assert.Throws<ArgumentNullException>(() => admin.CreateAcls(null!)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => admin.CreateAcls(new AclBinding?[] { Binding("ok"), null }!));
        Assert.Equal("acls", nullElement.ParamName);
        Assert.StartsWith(
            "The ACL bindings must not contain a null element.", nullElement.Message, StringComparison.Ordinal);

        Assert.Null(Record.Exception(
            () => admin.CreateAcls(
                new[] { Binding("ok") }, new CreateAclsOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.CreateAcls(new[] { Binding("gone") }));
    }

    [Fact]
    public void Options_DefaultToJavasDefaults() => Assert.Null(new CreateAclsOptions().TimeoutMs);

    private static AclBinding Binding(string name) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
